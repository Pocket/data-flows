"""Query based data extraction from Snowflake"""
import os
from asyncio import run
from copy import deepcopy
from pathlib import Path
from typing import Optional

from common import get_script_path
from common.cloud.gcp_utils import PktGcpCredentials
from common.databases.snowflake_utils import PktSnowflakeConnector, get_gcs_stage
from common.deployment import FlowDeployment, FlowEnvar, FlowSpec
from common.settings import CommonSettings, get_deployment_type_setting
from pendulum import now
from prefect import flow, get_run_logger, task
from prefect_gcp.bigquery import bigquery_query
from prefect_snowflake.database import snowflake_query
from pydantic import BaseModel, Field

CS = CommonSettings()  # type: ignore

DEFAULT_STAGE = get_gcs_stage()


class BqExtractSfLoadJob(BaseModel):
    """Model to hold SQL instructions for extract of data from Big Query
    and loading of data into Snowflake."""

    extraction_sql: str = Field(..., description="Rendered extraction SQL")
    load_sql: str = Field(..., description="Rendered load SQL")


class BqExtractSfLoadInputs(BaseModel):
    """Model for parameters to passed to an extract job request."""

    sql_name: str = Field(
        ...,
        description="Relative folder name containing queries",
    )
    default_offset: Optional[str] = Field(
        "no-value",
        description="Default offset value to use for new jobs or empty state",
    )
    kwargs: dict = Field(
        {},
        description=(
            "Any additional keyword arguments as a dictionary"
            "to pass into your templates"
        ),
    )

    def _get_sql_template(self, file_name: str) -> str:
        # get the query to use as extraction source
        script_path = get_script_path()
        sql_template = Path(
            f"{script_path}/sql/{self.sql_name}/{file_name}.sql"
        ).read_text()
        # render the query using inputs
        return sql_template

    def get_extract_sql(self, current_offset: str) -> str:
        kwargs_copy = deepcopy(self.kwargs)
        kwargs_copy["current_offset"] = current_offset
        return self._get_sql_template("extract").format(**kwargs_copy)

    def get_offset_sql(self) -> str:
        kwargs_copy = deepcopy(self.kwargs)
        kwargs_copy["default_offset"] = self.default_offset
        return self._get_sql_template("offset").format(**kwargs_copy)

    def get_load_sql(self, snowflake_stage_uri: str, batch_id: int) -> str:
        kwargs_copy = deepcopy(self.kwargs)
        kwargs_copy["snowflake_stage_uri"] = snowflake_stage_uri
        kwargs_copy["batch_id"] = str(batch_id)
        kwargs_copy[
            "metadata_keys"
        ] = """_gs_file_name,
            _gs_file_row_number,
            _gs_file_year,
            _gs_file_month,
            _gs_file_day,
            _gs_file_hour,
            _gs_file_unix,
            _loaded_at"""
        kwargs_copy[
            "metadata_values"
        ] = """metadata$filename,
            metadata$file_row_number,
            split_part(metadata$filename,'/', -6),
            split_part(metadata$filename,'/', -5),
            split_part(metadata$filename,'/', -4),
            split_part(metadata$filename,'/', -3),
            split_part(metadata$filename,'/', -2),
            sysdate()"""
        return self._get_sql_template("load").format(**kwargs_copy)


@task()
def get_pocket_snowflake_connector_block():
    return PktSnowflakeConnector(warehouse=f"prefect_wh_{CS.dev_or_production}")


@task()
def create_extract_load_job(
    extract_load_input: BqExtractSfLoadInputs, current_offset: str
) -> BqExtractSfLoadJob:
    """Task to create the BqExtractSfLoadJob model from the flow parameters.

    Args:
        extract_load_input (BqExtractSfLoadInputs): Flow paramters model.
        current_offset (str): Current offset for extraction.

    Returns:
        BqExtractSfLoadJob: Model with SQL instructions.
    """

    # template for wrapping a SQL query into unload statement
    extraction_sql_template = """EXPORT DATA OPTIONS(
          uri='{gcs_uri}',
          format='PARQUET',
          compression='SNAPPY',
          overwrite=true) AS
    {sql}"""

    # leverage UTC datetime for file partitioning
    ts = now(tz="utc")
    unix_ts = ts.int_timestamp
    partition = os.path.join(
        extract_load_input.sql_name,
        str(ts.year),
        str(ts.month),
        str(ts.day),
        str(ts.hour),
        str(unix_ts),
    )
    gcs_uri = os.path.join(DEFAULT_STAGE.stage_location, partition, "data*.parq")
    snowflake_stage_uri = f"@{os.path.join(DEFAULT_STAGE.stage_name, partition)}"
    # return the extract job for downstream usage
    return BqExtractSfLoadJob(
        extraction_sql=extraction_sql_template.format(
            gcs_uri=gcs_uri,
            sql=extract_load_input.get_extract_sql(current_offset=current_offset),
        ),
        load_sql=extract_load_input.get_load_sql(
            snowflake_stage_uri=snowflake_stage_uri, batch_id=unix_ts
        ),
    )


@flow(
    description="""Main workflow for orchestration of query based extractions
from Big Query and importing into Snowflake.
"""
)
async def main(extract_load_input: BqExtractSfLoadInputs):
    # get standard Prefect logger for logging
    logger = get_run_logger()
    # get reusable snowflake connector block for pocket
    sfc = get_pocket_snowflake_connector_block()
    # get the current offset using input model property
    current_offset = await snowflake_query.with_options(  # type: ignore
        name="get-current-offset"
    )(
        query=extract_load_input.get_offset_sql(),
        snowflake_connector=sfc,
    )
    logger.info(f"Current offset state result: {current_offset[0][0]}...")
    # get the extract_load_job details
    extract_load_job = create_extract_load_job(extract_load_input, current_offset[0][0])
    logger.info("Running extraction from Big Query...")
    # run the extraction
    extract = await bigquery_query.with_options(name="run-extraction")(  # type: ignore
        query=extract_load_job.extraction_sql,
        gcp_credentials=PktGcpCredentials(),
    )
    logger.info("Loading into Snowflake...")
    # commit the new offset
    await snowflake_query.with_options(name="run-load")(  # type: ignore
        query=extract_load_job.load_sql,
        snowflake_connector=sfc,
        wait_for=[extract],
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
    ],
    deployments=[
        FlowDeployment(
            deployment_name="impression_stats_v1",
            parameters={
                "extract_load_input": BqExtractSfLoadInputs(
                    sql_name="impression_stats_v1",
                    kwargs={"table_name": "impression_stats_v1_dev"},
                ).dict()
            },
            envars=[
                FlowEnvar(
                    envar_name="DF_CONFIG_SNOWFLAKE_SCHEMA",
                    envar_value=get_deployment_type_setting(
                        dev="braun", staging="braun", main="mozilla"
                    ),
                )
            ],
        )  # type: ignore
    ],
)


if __name__ == "__main__":
    t = BqExtractSfLoadInputs(
        sql_name="impression_stats_v1",
        kwargs={"table_name": "impression_stats_v1_dev"},
    )
    run(main(extract_load_input=t))  # type: ignore
