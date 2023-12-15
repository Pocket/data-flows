import os
from copy import deepcopy
from pathlib import Path

import pendulum as pdm
from common import get_script_path
from common.databases.snowflake_utils import (
    SfGcsStage,
    SnowflakeGcsStageSettings,
    get_gcs_stage,
)
from common.deployment import FlowDeployment, FlowEnvar, FlowSpec
from common.settings import CommonSettings, get_cached_settings
from prefect import flow, get_run_logger
from prefect.server.schemas.schedules import CronSchedule
from shared.async_utils import process_parallel_subflows
from shared.utils import (
    IntervalSet,
    SharedUtilsSettings,
    SqlJob,
    SqlStmt,
    get_files_for_cleanup,
)

CS = CommonSettings()  # type: ignore

# template sql to get the latest stored offset for etl job
LAST_OFFSET_SQL = """{% set sql_engine = "snowflake" %}
    select any_value(last_offset) as last_offset
    from sql_offset_state
    where sql_folder_name = '{{ sql_folder_name }}';"""

# template sql to persist the new offset for etl job
PERSIST_STATE_SQL = """{% set sql_engine = "snowflake" %}
    merge into sql_offset_state dt using (
    select '{{ sql_folder_name }}' as sql_folder_name, 
    current_timestamp as created_at, 
    current_timestamp as updated_at,
    '{{ new_offset }}' as last_offset
    ) st on st.sql_folder_name = dt.sql_folder_name
    when matched then update 
    set updated_at = st.updated_at,
        last_offset = st.last_offset
    when not matched then insert (sql_folder_name, created_at, updated_at, last_offset) 
    values (st.sql_folder_name, st.created_at, st.updated_at, st.last_offset);"""

# template sql for removing files from stage
REMOVE_FILE_SQL = (
    '{% set sql_engine = "snowflake" %}'
    "REMOVE '@{{ stage_name }}/{{ sql_folder_name }}/{{ old_partition_folders }}'"
)

# template sql for listing files in stage
EXISTING_FILES_SQL = (
    '{% set sql_engine = "snowflake" %}'
    "LIST '@{{ stage_name }}/{{ sql_folder_name }}/{{ partition_date_folder }}'"
)


class SqlEtlJob(SqlJob):
    """Model for parameters to passed to an etl job request."""

    snowflake_stage_id: str = "default"
    with_external_state: bool = False

    def _init_private_attributes(self) -> None:
        """Overriding the private attributes set to include
        os.path.join(get_script_path(), "sql") as a default.
        """
        super()._init_private_attributes()
        self._sql_template_path = SharedUtilsSettings().sql_template_path or os.path.join(get_script_path(), "sql")  # type: ignore  # noqa: E501

    @property
    def has_extraction_sql(self):
        """Property for initiating case logic based on existence
        of data.sql file.

        Returns:
            bool: Does file exist or not.
        """
        return Path(os.path.join(self.job_file_path, "data.sql")).exists()

    @property
    def has_load_sql(self):
        """Property for initiating case logic based on existence
        of load.sql file.

        Returns:
            bool: Does file exist or not.
        """
        return Path(os.path.join(self.job_file_path, "load.sql")).exists()

    @property
    def is_incremental(self):
        """Logic for this property is based on existence of offset.sql
        or with_external_state property.

        Returns:
            bool: is incremental?
        """
        return (
            Path(os.path.join(self.job_file_path, "offset.sql")).exists()
            or self.with_external_state
        )

    @property
    def default_table_name(self):
        """Default table name based on sql folder that can be used.

        Returns:
            str: default table name
        """
        return self.sql_folder_name.split("/")[-1]

    @property
    def snowflake_stage(self) -> SfGcsStage:
        """Get Snowflake Gcp Stage to use based on deployment type.

        Returns:
            SfGcsStage: Model for stage metadata.
        """
        stg_data = get_cached_settings(SnowflakeGcsStageSettings)
        return get_gcs_stage(stg_data.snowflake_gcp_stage_data, self.snowflake_stage_id)

    def get_gcs_uri(self, interval_input: IntervalSet) -> str:
        """Get the Gcp storage uri based on interval metadata.

        Args:
            interval_input (IntervalSet): Interval metadata.

        Returns:
            str: Full gcp storage uri as string.
        """
        return os.path.join(
            self.snowflake_stage.stage_location,
            self.sql_folder_name,
            interval_input.partition_folders,
            "data*.parq",
        )

    def get_snowflake_stage_uri(self, interval_input: IntervalSet) -> str:
        """Get the Snowflake stage uri based on interval metadata.

        Args:
            interval_input (IntervalSet): Interval metadata.

        Returns:
            str: str: Full Snowflake stage uri as string.
        """
        return f"@{os.path.join(self.snowflake_stage.stage_name, self.sql_folder_name, interval_input.partition_folders)}"  # noqa: E501

    def get_last_offset_sql(self) -> SqlStmt:
        """Provide rendered current offset SQL to be executed.

        Returns:
            (SqlStmt): SqlStmt object with sql text and db engine.
        """
        if self.with_external_state:
            return self.render_sql_string(LAST_OFFSET_SQL)
        else:
            return self.render_sql_file("offset.sql")

    def get_extraction_sql(self, interval_input: IntervalSet) -> SqlStmt:
        """Provide rendered query to be passed for extraction.

        Args:
            interval_input (IntervalSet): Interval metadata.

        Returns:
            (SqlStmt): SqlStmt object with sql text and db engine.
        """
        # templates for wrapping a SQL query into unload statement
        sf_extraction_sql = """{% set sql_engine = "snowflake" %}
        copy into '{{ snowflake_stage_uri }}/data'
        from ({{ sql }})
        header = true
        overwrite = true
        max_file_size = 104857600"""  # noqa: E501

        bq_extraction_sql = """{% set sql_engine = "bigquery" %}
        EXPORT DATA OPTIONS(
          uri='{{ gcs_uri }}',
          format='PARQUET',
          compression='SNAPPY',
          overwrite=true) AS
        {{ sql }}"""  # noqa: E501

        source_mapping = {"snowflake": sf_extraction_sql, "bigquery": bq_extraction_sql}

        extra_kwargs = {
            "snowflake_stage_uri": self.get_snowflake_stage_uri(interval_input),
            "gcs_uri": self.get_gcs_uri(interval_input),
        }
        extra_kwargs.update(interval_input.dict())
        sql_query = self.render_sql_file("data.sql", extra_kwargs=extra_kwargs)
        extra_kwargs["sql"] = sql_query.sql_text
        # only wrapping snowflake and bigquery in export logic
        if x := source_mapping.get(sql_query.sql_engine):
            extraction_sql_stmt = self.render_sql_string(x, extra_kwargs=extra_kwargs)
        else:
            extraction_sql_stmt = sql_query
        return extraction_sql_stmt

    def get_new_offset_sql(self, interval_input: IntervalSet) -> SqlStmt:
        """Provide rendered new offset SQL to be executed.

        Args:
            interval_input (IntervalSet): Interval metadata.

        Returns:
            (SqlStmt): SqlStmt object with sql text and db engine.
        """
        extra_kwargs = {"for_new_offset": True}
        extra_kwargs.update(interval_input.dict())
        return self.render_sql_file("data.sql", extra_kwargs)

    def get_persist_offset_sql(self, new_offset: str) -> SqlStmt:
        """Provide rendered persist offset SQL to be executed.

        Args:
            new_offset (str): New offset to persist to external state.

        Returns:
            (SqlStmt): SqlStmt object with sql text and db engine.
        """
        return self.render_sql_string(PERSIST_STATE_SQL, {"new_offset": new_offset})

    def get_file_list_sql(self, interval_input: IntervalSet) -> SqlStmt:
        """Provide rendered list files in stage SQL to be executed.

        Args:
            interval_input (IntervalSet): Interval metadata.

        Returns:
            (SqlStmt): SqlStmt object with sql text and db engine.
        """
        extra_kwargs = {
            "stage_name": self.snowflake_stage,
            "partition_date_folder": interval_input.partition_date_folder,
        }
        return self.render_sql_string(EXISTING_FILES_SQL, extra_kwargs)

    def get_file_remove_sql(self, old_partition_folders: str) -> SqlStmt:
        """Provide rendered SQL to be remove files from stage.

        Args:
            old_partition_folders (str): file suffix to use for removal.

        Returns:
            (SqlStmt): SqlStmt object with sql text and db engine.
        """
        extra_kwargs = {
            "stage_name": self.snowflake_stage,
            "old_partition_folders": old_partition_folders,
        }
        return self.render_sql_string(REMOVE_FILE_SQL, extra_kwargs)

    def get_load_sql(self, interval_input: IntervalSet) -> SqlStmt:
        """Provide rendered SQL for post extract load.

        Args:
            interval_input (IntervalSet): Interval metadata.

        Returns:
            (SqlStmt): SqlStmt object with sql text and db engine.
        """
        load_sql_file_name = "load.sql"
        extra_kwargs = {
            "snowflake_stage_uri": self.get_snowflake_stage_uri(interval_input),
            "partition_timestamp": interval_input.partition_timestamp,
            "metadata_column_definitions": """_gs_file_name string,
            _gs_file_row_number number,
            _gs_file_date string,
            _gs_file_time string,
            _loaded_at timestamp_tz""",
            "metadata_keys": """_gs_file_name,
            _gs_file_row_number,
            _gs_file_date,
            _gs_file_time,
            _loaded_at""",
            "metadata_values": """metadata$filename,
            metadata$file_row_number,
            split_part(metadata$filename,'/', -3),
            split_part(metadata$filename,'/', -2),
            sysdate()""",
            "table_name": self.default_table_name,
        }
        extra_kwargs.update(interval_input.dict())
        return self.render_sql_file(load_sql_file_name, extra_kwargs)


@flow(description="Interval flow for query based extractions from Snowflake.")
async def interval(etl_input: SqlEtlJob, interval_input: IntervalSet):
    """Subflow for executing etl tasks for a single interval.
    Each query call will leverage run_query_task helper function
    to automate sending query to proper engine.

    Args:
        etl_input (SqlEtlJob): Sql job input parameters.
        interval_input (IntervalSet): Interval set metadata.
    """
    # get standard Prefect logger for logging
    logger = get_run_logger()
    # preventing unbound error for new_offset usage downstream
    new_offset = [("1970-01-01",)]
    # if is incremental, track new offset
    # get the new offset using input model property
    if etl_input.is_incremental:
        offset_stmt = etl_input.get_new_offset_sql(interval_input)
        new_offset = await offset_stmt.run_query_task("get-new-offset")
        logger.info(f"New offset will be: {new_offset[0][0]}...")
        # if new offset is None, that means no rows for this interval
        if new_offset[0][0] is None or new_offset[0][0] == "None":
            message = "No rows to process..."
            logger.info(message)
            return message
        # based on the starting offset, find list of object paths to delete when...
        # doing backfill
        existing_files_stmt = etl_input.get_file_list_sql(interval_input)
        existing_files = await existing_files_stmt.run_query_task("get-existing-files")
        # take the LIST statement results and provide clean deduplicated list
        clean_up_list = get_files_for_cleanup(existing_files, interval_input)
        # remove all the object paths identified

        async def remove_file(file_path: str):
            """internal function to wrap remove logic to be
            used for collection of async calls.

            Args:
                file_path (str): Snowflake stage path string.
            """
            sql_stmt = etl_input.get_file_remove_sql(file_path)
            await sql_stmt.run_query_task("clean-up-files")

        remove_files = [await remove_file(i) for i in clean_up_list]
    else:
        logger.info("Non-incremental run...")
        logger.info("No offsets...")
        # need a remove_file for downstream continuation
        remove_files = []
    # Run extraction
    logger.info("Running extraction...")
    extract_stmt = etl_input.get_extraction_sql(interval_input)
    extract = await extract_stmt.run_query_task(
        "run-extraction", **{"wait_for": [remove_files]}
    )
    # run a post extraction load sql if it exists
    if etl_input.has_load_sql:
        logger.info("Applying new offset...")
        # commit the new offset
        load_sql = etl_input.get_load_sql(interval_input)
        load = await load_sql.run_query_task("run-load", **{"wait_for": [extract]})
        logger.info(f"Load logic completed with: {load[0]}")
    else:
        load = "No load sql to execute..."
        logger.info(load)
    # persist the new offset to external snowflake state table if enabled
    if etl_input.is_incremental:
        if etl_input.with_external_state:
            logger.info("Applying new offset...")
            # commit the new offset
            persist_offset_stmt = etl_input.get_persist_offset_sql(new_offset[0][0])
            persist_offset = await persist_offset_stmt.run_query_task(
                "persist-new-offset", **{"wait_for": [load]}
            )
            logger.info(f"Persist offset logic completed with: {persist_offset[0]}")
        else:
            persist_offset = "External state disabled. No offset to persist..."
            logger.info(persist_offset)


@flow(description="Interval flow for query based extractions from Snowflake.")
async def main(etl_input: SqlEtlJob):
    """Main flow for iterating through etl intervals.

    Args:
        etl_input (SqlEtlJob): Sql job input parameters.
    """
    # get standard Prefect logger for logging
    logger = get_run_logger()

    # helper functions to reduce code
    async def process_intervals(etl_input):
        if etl_input.has_extraction_sql:
            if etl_input.is_incremental:
                logger.info(
                    f"Running directory: {etl_input.sql_folder_name}..."  # noqa: E501
                )
                # get the last offset
                last_offset_stmt = etl_input.get_last_offset_sql()
                last_offset = await last_offset_stmt.run_query_task("get-last-offset")
                logger.info(f"Last offset is: {last_offset[0][0]}...")
                for i in etl_input.get_intervals(last_offset[0][0]):
                    await interval(etl_input, i)
            else:
                static_datetime_str = (
                    etl_input.override_last_offset
                    or pdm.now(tz="UTC").to_iso8601_string()
                )
                static_interval = IntervalSet(
                    batch_start=static_datetime_str
                )  # type: ignore
                await interval(etl_input, static_interval)

        else:
            logger.info(f"No extraction for directory: {etl_input.sql_folder_name}...")

    async def process_all():
        sub_paths = [
            path.parts[-1]
            for path in Path(etl_input.job_file_path).iterdir()
            if path.is_dir()
        ]
        if sub_paths:
            logger.info("Sub directories exist...")
            task_group = []
            for s in sub_paths:
                new_input = deepcopy(etl_input)
                new_folder_name = os.path.join(etl_input.sql_folder_name, s)
                new_input.sql_folder_name = new_folder_name
                logger.info(f"Running for sub directory: {new_folder_name}...")
                task_group.append(process_intervals(new_input))
            await process_parallel_subflows(task_group)
        await process_intervals(etl_input)

    await process_all()


# helper for passing stage id to deployment parameters
SF_GCP_STAGE_ID = CS.deployment_type_value(
    dev="default", staging="default", main="gcs_pocket_shared"
)

FLOW_SPEC = FlowSpec(
    flow=main,
    docker_env="base",
    secrets=[
        FlowEnvar(
            envar_name="DF_CONFIG_SNOWFLAKE_CREDENTIALS",
            envar_value=f"data-flows/{CS.deployment_type}/snowflake-credentials",
        ),
        FlowEnvar(
            envar_name="DF_CONFIG_GCP_CREDENTIALS",
            envar_value=f"data-flows/{CS.deployment_type}/gcp-credentials",
        ),
        FlowEnvar(
            envar_name="DF_CONFIG_SNOWFLAKE_GCP_STAGES",
            envar_value=f"data-flows/{CS.deployment_type}/snowflake-gcp-stage-data",
        ),
        FlowEnvar(
            envar_name="DF_CONFIG_SQLALCHEMY_CREDENTIALS",
            envar_value=f"data-flows/{CS.deployment_type}/aurora-rds",
        ),
    ],
    deployments=[
        FlowDeployment(
            deployment_name="backend_events_for_mozilla",
            schedule=CronSchedule(cron="0 1 * * *", timezone="America/Los_Angeles"),
            parameters={
                "etl_input": SqlEtlJob(
                    sql_folder_name="backend_events_for_mozilla",
                    initial_last_offset="2023-06-18 23:59:59.999",
                    kwargs={
                        "database_name": "snowplow",
                        "schema_name": "atomic",
                        "table_name": "events",
                    },
                    with_external_state=True,
                    snowflake_stage_id=SF_GCP_STAGE_ID,
                ).dict()  # type: ignore
            },
            envars=[
                FlowEnvar(
                    envar_name="DF_CONFIG_SNOWFLAKE_SCHEMA",
                    envar_value=CS.deployment_type_value(
                        dev="braun", staging="staging", main="public"
                    ),  # type: ignore
                ),
            ],
        ),
        FlowDeployment(
            deployment_name="curated_feed_exports_aurora",
            schedule=CronSchedule(cron="0 * * * *"),
            parameters={
                "etl_input": SqlEtlJob(
                    sql_folder_name="curated_feed_exports_aurora",
                    kwargs={
                        "environment": CS.deployment_type,
                    },
                ).dict()  # type: ignore
            },
            envars=[
                FlowEnvar(
                    envar_name="DF_CONFIG_SNOWFLAKE_SCHEMA",
                    envar_value=CS.deployment_type_value(
                        dev="braun", staging="staging", main="mysql"
                    ),  # type: ignore
                ),
            ],
        ),
        FlowDeployment(
            deployment_name="firefox_new_tab_impressions_daily",
            schedule=CronSchedule(cron="0 6 * * *"),
            parameters={
                "etl_input": SqlEtlJob(
                    sql_folder_name="firefox_new_tab_impressions_daily"
                ).dict()  # type: ignore
            },
            envars=[
                FlowEnvar(
                    envar_name="DF_CONFIG_SNOWFLAKE_SCHEMA",
                    envar_value=CS.deployment_type_value(
                        dev="braun", staging="staging", main="mozilla"
                    ),  # type: ignore
                ),
            ],
        ),
        FlowDeployment(
            deployment_name="firefox_new_tab_impressions_hourly",
            schedule=CronSchedule(cron="0 * * * *"),
            parameters={
                "etl_input": SqlEtlJob(
                    sql_folder_name="firefox_new_tab_impressions_hourly"
                ).dict()  # type: ignore
            },
            envars=[
                FlowEnvar(
                    envar_name="DF_CONFIG_SNOWFLAKE_SCHEMA",
                    envar_value=CS.deployment_type_value(
                        dev="braun", staging="staging", main="mozilla"
                    ),  # type: ignore
                ),
            ],
        ),
        FlowDeployment(
            deployment_name="glean_firefox_new_tab_impressions_daily",
            schedule=CronSchedule(cron="0 6 * * *"),
            parameters={
                "etl_input": SqlEtlJob(
                    sql_folder_name="glean_firefox_new_tab_impressions_daily"
                ).dict()  # type: ignore
            },
            envars=[
                FlowEnvar(
                    envar_name="DF_CONFIG_SNOWFLAKE_SCHEMA",
                    envar_value=CS.deployment_type_value(
                        dev="cbeck", staging="staging", main="mozilla"
                    ),  # type: ignore
                ),
            ],
        ),
        FlowDeployment(
            deployment_name="glean_firefox_new_tab_impressions_hourly",
            schedule=CronSchedule(cron="0 * * * *"),
            parameters={
                "etl_input": SqlEtlJob(
                    sql_folder_name="glean_firefox_new_tab_impressions_hourly"
                ).dict()  # type: ignore
            },
            envars=[
                FlowEnvar(
                    envar_name="DF_CONFIG_SNOWFLAKE_SCHEMA",
                    envar_value=CS.deployment_type_value(
                        dev="cbeck", staging="staging", main="mozilla"
                    ),  # type: ignore
                ),
            ],
        ),
    ],
)


if __name__ == "__main__":
    from asyncio import run

    t = SqlEtlJob(
        sql_folder_name="glean_firefox_new_tab_impressions_hourly/glean_firefox_new_tab_daily_engagement_by_tile_id_position_country_locale",
        kwargs={"for_backfill": False},
        # override_last_offset="2023-12-04 23:59:59.999999",
    )  # type: ignore
    run(main(etl_input=t))  # type: ignore
