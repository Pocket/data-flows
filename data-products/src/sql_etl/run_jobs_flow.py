import os
from datetime import timedelta
from pathlib import Path
from typing import Literal, Union

from common import get_script_path
from common.cloud.gcp_utils import PktGcpCredentials
from common.databases.snowflake_utils import (
    PktSnowflakeConnector,
    SfGcsStage,
    get_gcs_stage,
    get_pocket_snowflake_connector_block,
)
from common.deployment import FlowDeployment, FlowEnvar, FlowSpec
from common.settings import CommonSettings
from pendulum.parser import parse
from prefect import flow, get_run_logger
from prefect.server.schemas.schedules import IntervalSchedule
from prefect_gcp.bigquery import bigquery_query
from prefect_snowflake.database import snowflake_query
from shared.utils import IntervalSet, SharedUtilsSettings, SqlJob, get_files_for_cleanup

CS = CommonSettings()  # type: ignore

# template sql to get the latest stored offset for etl job
LAST_OFFSET_SQL = """select any_value(last_offset) as last_offset
    from sql_offset_state
    where sql_folder_name = '{{ sql_folder_name }}';"""

# template sql to persist the new offset for etl job
PERSIST_STATE_SQL = """merge into sql_offset_state dt using (
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
    "REMOVE '@{{ stage_name }}/{{ sql_folder_name }}/{{ old_partition_folders }}'"
)

# template sql for listing files in stage
EXISTING_FILES_SQL = (
    "LIST '@{{ stage_name }}/{{ sql_folder_name }}/{{ partition_date_folder }}'"
)


class SqlEtlJob(SqlJob):
    """Model for parameters to passed to an etl job request."""

    snowflake_stage_id: str = "default"
    source_system: Literal["snowflake", "bigquery"]
    with_external_state: bool = False
    warehouse_override: Union[str, None] = None

    def _init_private_attributes(self) -> None:
        """Overriding the private attributes set to include
        os.path.join(get_script_path(), "sql") as a default.
        """
        super()._init_private_attributes()
        self._sql_template_path = SharedUtilsSettings().sql_template_path or os.path.join(get_script_path(), "sql")  # type: ignore  # noqa: E501

    @property
    def snowflake_stage(self) -> SfGcsStage:
        """Get Snowflake Gcp Stage to use based on deployment type.

        Returns:
            SfGcsStage: Model for stage metadata.
        """
        return get_gcs_stage(self.snowflake_stage_id)

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

    def get_last_offset_sql(self) -> str:
        """Provide rendered current offset SQL to be executed.

        Returns:
            str: Rendered current offset SQL.
        """
        if self.with_external_state:
            return self.render_sql_string(LAST_OFFSET_SQL)
        else:
            return self.render_sql_file("offset.sql")

    def get_extraction_sql(self, interval_input: IntervalSet) -> str:
        """Provide rendered query to be passed for extraction.

        Args:
            interval_input (IntervalSet): Interval metadata.

        Returns:
            str: Rendered extraction SQL.
        """
        # templates for wrapping a SQL query into unload statement
        sf_extraction_sql = """copy into '{{ snowflake_stage_uri }}/data'
        from ({{ sql }})
        header = true
        overwrite = true
        max_file_size = 104857600"""  # noqa: E501

        bq_extraction_sql = """EXPORT DATA OPTIONS(
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
        extra_kwargs["sql"] = sql_query
        extraction_sql_stmt = self.render_sql_string(
            source_mapping[self.source_system], extra_kwargs=extra_kwargs
        )

        return extraction_sql_stmt

    def get_new_offset_sql(self, interval_input: IntervalSet) -> str:
        """Provide rendered new offset SQL to be executed.

        Args:
            interval_input (IntervalSet): Interval metadata.

        Returns:
            str: Rendered new offset SQL.
        """
        extra_kwargs = {"for_new_offset": True}
        extra_kwargs.update(interval_input.dict())
        return self.render_sql_file("data.sql", extra_kwargs)

    def get_persist_offset_sql(self, new_offset: str) -> str:
        """Provide rendered persist offset SQL to be executed.

        Args:
            new_offset (str): New offset to persist to external state.

        Returns:
            str: Rendered persist SQL.
        """
        return self.render_sql_string(PERSIST_STATE_SQL, {"new_offset": new_offset})

    def get_file_list_sql(self, interval_input: IntervalSet) -> str:
        """Provide rendered list files in stage SQL to be executed.

        Args:
            interval_input (IntervalSet): Interval metadata.

        Returns:
            str: Rendered list file SQL.
        """
        extra_kwargs = {
            "stage_name": self.snowflake_stage,
            "partition_date_folder": interval_input.partition_date_folder,
        }
        return self.render_sql_string(EXISTING_FILES_SQL, extra_kwargs)

    def get_file_remove_sql(self, old_partition_folders: str) -> str:
        """Provide rendered SQL to be remove files from stage.

        Args:
            old_partition_folders (str): file suffix to use for removal.

        Returns:
            str: Rendered remove file SQL.
        """
        extra_kwargs = {
            "stage_name": self.snowflake_stage,
            "old_partition_folders": old_partition_folders,
        }
        return self.render_sql_string(REMOVE_FILE_SQL, extra_kwargs)

    def get_load_sql(self, interval_input: IntervalSet) -> Union[str, None]:
        """Provide rendered SQL for post extract load.
        Will only return if load.sql exists.

        Args:
            interval_input (IntervalSet): Interval metadata.

        Returns:
            Union[str, None]: Rendered load or None
        """
        load_sql_file_name = "load.sql"
        sql_template_path = self._sql_template_path
        if not Path(
            os.path.join(
                sql_template_path,  # type: ignore
                self.sql_folder_name,  # type: ignore
                load_sql_file_name,  # type: ignore
            )
        ).exists():
            return None
        extra_kwargs = {
            "snowflake_stage_uri": self.get_snowflake_stage_uri(interval_input),
            "partition_timestamp": interval_input.partition_timestamp,
            "metadata_keys": """_gs_file_name,
            _gs_file_row_number,
            _gs_file_date,
            _gs_file_time,
            _loaded_at""",
            "metadata_values": """metadata$filename,
            metadata$file_row_number,
            split_part(metadata$filename,'/', -2),
            split_part(metadata$filename,'/', -1),
            sysdate()""",
        }
        return self.render_sql_file(load_sql_file_name, extra_kwargs)


@flow(description="Interval flow for query based extractions from Snowflake.")
async def interval(
    etl_input: SqlEtlJob, interval_input: IntervalSet, sfc: PktSnowflakeConnector
):
    """Subflow for executing etl tasks for a single interval

    Args:
        etl_input (SqlEtlJob): Sql job input parameters.
        interval_input (IntervalSet): Interval set metadata.
        sfc (PktSnowflakeConnector): Proper Snowflake Connector to pass downstream
        from main flow.
    """
    # get standard Prefect logger for logging
    logger = get_run_logger()
    # get the new offset using input model property
    # need to switch between source system query tasks
    if etl_input.source_system == "snowflake":
        new_offset = await snowflake_query.with_options(  # type: ignore
            name="get-new-offset"
        )(
            query=etl_input.get_new_offset_sql(interval_input),
            snowflake_connector=sfc,
        )
    else:
        new_offset = await bigquery_query.with_options(  # type: ignore
            name="get-new-offset"
        )(
            query=etl_input.get_new_offset_sql(interval_input),
            gcp_credentials=PktGcpCredentials(),
        )
    logger.info(f"New offset will be: {new_offset[0][0]}...")
    # if new offset is None, that means no rows for this interval
    if new_offset[0][0] is None or new_offset[0][0] == "None":
        message = "No rows to process..."
        logger.info(message)
        return message
    # based on the starting offset, find list of object paths to delete when...
    # doing backfill
    existing_files = await snowflake_query.with_options(name="get-existing-files")(
        query=etl_input.get_file_list_sql(interval_input),
        snowflake_connector=sfc,
    )
    # take the LIST statement results and provide clean deduplicated list
    clean_up_list = get_files_for_cleanup(existing_files, interval_input)
    # remove all the object paths identified
    remove_files = [
        await snowflake_query.with_options(name="clean-up-files")(  # type: ignore
            query=etl_input.get_file_remove_sql(i),
            snowflake_connector=sfc,
        )
        for i in clean_up_list
    ]
    # Run extraction
    logger.info("Running extraction...")
    if etl_input.source_system == "snowflake":
        extract = await snowflake_query.with_options(name="run-extraction")(  # type: ignore  # noqa: E501
            query=etl_input.get_extraction_sql(interval_input),
            snowflake_connector=sfc,
            wait_for=[remove_files],
        )
        logger.info(f"Extract logic completed with: {extract[0]}")
    else:
        extract = await bigquery_query.with_options(name="run-extraction")(  # type: ignore  # noqa: E501
            query=etl_input.get_extraction_sql(interval_input),
            gcp_credentials=PktGcpCredentials(),
            wait_for=[remove_files],
        )
    # run a post extraction load sql if it exists
    if x := etl_input.get_load_sql(interval_input):
        logger.info("Applying new offset...")
        # commit the new offset
        load = await snowflake_query.with_options(name="run-load")(  # type: ignore  # noqa: E501
            query=x,
            snowflake_connector=sfc,
            wait_for=[extract],
        )
        logger.info(f"Load logic completed with: {load[0]}")
    else:
        load = "No load sql to execute..."
        logger.info(load)
    # persist the new offset to external snowflake state table if enabled
    if etl_input.with_external_state:
        logger.info("Applying new offset...")
        # commit the new offset
        persist_offset = await snowflake_query.with_options(name="persist-new-offset")(  # type: ignore  # noqa: E501
            query=etl_input.get_persist_offset_sql(new_offset[0][0]),
            snowflake_connector=sfc,
            wait_for=[load],
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
    # get reusable snowflake connector block for pocket
    sfc = get_pocket_snowflake_connector_block(
        warehouse_override=etl_input.warehouse_override
    )
    # get the last offset
    last_offset = await snowflake_query.with_options(  # type: ignore
        name="get-last-offset"
    )(
        query=etl_input.get_last_offset_sql(),
        snowflake_connector=sfc,
    )
    logger.info(f"Last offset is: {last_offset[0][0]}...")
    # iterate through the intervals and perform subflow
    for i in etl_input.get_intervals(last_offset[0][0]):
        await interval(etl_input, i, sfc)


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
    ],
    deployments=[
        FlowDeployment(
            deployment_name="backend_events_for_mozilla",
            schedule=IntervalSchedule(
                interval=timedelta(days=1),
                anchor_date=parse("2023-06-27 01:00:00"),  # type: ignore
                timezone="America/Los_Angeles",
            ),
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
                    source_system="snowflake",
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
            deployment_name="impression_stats_v1",
            schedule=IntervalSchedule(
                interval=timedelta(minutes=10),
                anchor_date=parse("2023-06-27 01:00:00"),  # type: ignore
                timezone="America/Los_Angeles",
            ),
            parameters={
                "etl_input": SqlEtlJob(
                    sql_folder_name="impression_stats_v1_new",
                    initial_last_offset="2022-12-23",
                    kwargs={"destination_table_name": "impression_stats_v1"},
                    source_system="bigquery",
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
        ),  # type: ignore
    ],
)


if __name__ == "__main__":
    from asyncio import run

    t = SqlEtlJob(
        sql_folder_name="backend_events_for_mozilla",
        initial_last_offset="2023-06-18 23:59:59.999",
        kwargs={
            "database_name": "snowplow",
            "schema_name": "atomic",
            "table_name": "events",
        },
        with_external_state=True,
        source_system="snowflake",
        snowflake_stage_id=SF_GCP_STAGE_ID,
    )  # type: ignore
    run(main(etl_input=t))  # type: ignore
