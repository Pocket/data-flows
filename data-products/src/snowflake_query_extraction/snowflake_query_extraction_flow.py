"""Query based data extraction from Snowflake"""
from asyncio import run
from pathlib import Path

from common import get_script_path
from common.databases.snowflake_utils import PktSnowflakeConnector, get_gcs_stage
from common.deployment import FlowDeployment, FlowEnvar, FlowSpec
from common.settings import CommonSettings
from pendulum import now
from prefect import flow, get_run_logger, task
from prefect_snowflake.database import snowflake_query
from pydantic import BaseModel, Field

CS = CommonSettings()  # type: ignore

# template sql to get the latest stored offset for extraction job
CURRENT_OFFSET_SQL = """select coalesce(any_value(state), '{default_offset}') as state
    from query_extraction_state
    where sql_name = '{sql_name}';"""

# template sql to persist the new offset for extraction job
PERSIST_STATE_SQL = """merge into query_extraction_state dt using (
        select '{sql_name}' as sql_name, 
        current_timestamp as created_at, 
        current_timestamp as updated_at,
        '{new_offset}' as state
    ) st on st.sql_name = dt.sql_name
    when matched then update 
    set updated_at = st.updated_at,
        state = st.state
    when not matched then insert (sql_name, created_at, updated_at, state) 
    values (st.sql_name, st.created_at, st.updated_at, st.state);"""


class SfExtractionJob(BaseModel):
    """Model to hold SQL instructions for extraction and offset storage."""

    extraction_sql: str = Field(..., description="Rendered extraction SQL")
    persist_state_sql: str = Field(..., description="Rendered SQL to commit new offset")


class SfExtractionInputs(BaseModel):
    """Model for parameters to passed to an extraction job request."""

    sql_name: str = Field(
        ...,
        description="Relative folder name containing extraction query and offset query",
    )
    offset_key: str = Field(
        ...,
        description="Offset key to be passed in as needed to query and offset template",
    )
    default_offset: str = Field(
        ...,
        description="Default offset value to use for new extractions or empty state",
    )
    kwargs: dict = Field(
        {},
        description=(
            "Any additional keyword arguments as a dictionary"
            "to pass into your templates"
        ),
    )

    @property
    def current_offset_sql(self) -> str:
        """Provide Rendered current offset SQL to be executed.

        Returns:
            str: Rendered current offset SQL.
        """
        return CURRENT_OFFSET_SQL.format(
            sql_name=self.sql_name,
            offset_key=self.offset_key,
            default_offset=self.default_offset,
            **self.kwargs,
        )

    @property
    def new_offset_sql(self) -> str:
        """Provide Rendered new offset SQL to be executed.

        Returns:
            str: Rendered new offset SQL.
        """
        script_path = get_script_path()
        offset_sql_template = Path(
            f"{script_path}/sql/{self.sql_name}/offset.sql"
        ).read_text()
        return offset_sql_template.format(offset_key=self.offset_key, **self.kwargs)


@task()
def get_pocket_snowflake_connector_block():
    return PktSnowflakeConnector(
        schema="public", warehouse=f"prefect_wh_{CS.dev_or_production}"
    )


@task()
def create_extraction_job(
    sf_extraction_input: SfExtractionInputs, current_offset: str, new_offset: str
) -> SfExtractionJob:
    """Task to create the SfExtractionJob mode from the flow parameters and
    the current offset and expected new offset.

    Args:
        sf_extraction_input (SfExtractionInputs): Flow paramters model.
        current_offset (str): Current offset for extraction job.
        new_offset (str): The new offset to commit if extraction is successful.

    Returns:
        SfExtractionJob: Model with SQL instructions.
    """

    # template for wrapping a SQL query into unload statement
    extraction_sql_template = """copy into @{stage_name}/{sql_name}/{year}/{month}/{day}/{hour}/{unix}/data
    from ({sql})
    header = true
    overwrite = true
    max_file_size = 104857600"""  # noqa: E501

    # stage name must be one of our external GCS stages
    stage_name = get_gcs_stage()

    # leverage UTC datetime for file partitioning
    ts = now(tz="utc")

    # get the query to use as extraction source
    script_path = get_script_path()
    sql_text_template = Path(
        f"{script_path}/sql/{sf_extraction_input.sql_name}/data.sql"
    ).read_text()
    # render the query using inputs
    sql_text = sql_text_template.format(
        offset_key=sf_extraction_input.offset_key,
        current_offset=current_offset,
        new_offset=new_offset,
        **sf_extraction_input.kwargs,
    )
    # return the extract job for downstream usage
    return SfExtractionJob(
        extraction_sql=extraction_sql_template.format(
            stage_name=stage_name,
            sql_name=sf_extraction_input.sql_name,
            sql=sql_text,
            year=ts.year,
            month=ts.month,
            day=ts.day,
            hour=ts.hour,
            unix=ts.int_timestamp,
        ),
        persist_state_sql=PERSIST_STATE_SQL.format(
            sql_name=sf_extraction_input.sql_name, new_offset=new_offset
        ),
    )


@flow(
    description="""Main workflow for orchestration of query based extractions
from Snowflake.
"""
)
async def main(sf_extraction_input: SfExtractionInputs):
    # get standard Prefect logger for logging
    logger = get_run_logger()
    # get reusable snowflake connector block for pocket
    sfc = get_pocket_snowflake_connector_block()
    # get the current offset using input model property
    current_offset = await snowflake_query.with_options(  # type: ignore
        name="get-current-offset"
    )(
        query=sf_extraction_input.current_offset_sql,
        snowflake_connector=sfc,
    )
    logger.info(f"Current offset state result: {current_offset[0][0]}...")
    # get the new offset using input model property
    new_offset = await snowflake_query.with_options(  # type: ignore
        name="get-new-offset"
    )(
        query=sf_extraction_input.new_offset_sql,
        snowflake_connector=sfc,
    )
    logger.info(f"New offset will be: {new_offset[0][0]}...")
    # get the extraction job details
    extraction_job = create_extraction_job(
        sf_extraction_input, current_offset[0][0], new_offset[0][0]
    )
    logger.info("Running extraction...")
    # run the extraction
    extract = await snowflake_query.with_options(name="run-extraction")(  # type: ignore
        query=extraction_job.extraction_sql,
        snowflake_connector=sfc,
    )
    logger.info("Applying new state...")
    # commit the new offset
    await snowflake_query.with_options(name="persist-new-offset")(  # type: ignore
        query=extraction_job.persist_state_sql,
        snowflake_connector=sfc,
        wait_for=[extract],
    )


FLOW_SPEC = FlowSpec(
    flow=main,
    docker_env="base",
    secrets=[
        FlowEnvar(
            envar_name="DF_CONFIG_SNOWFLAKE_CREDENTIALS",
            envar_value=f"data-flows/{CS.dev_or_production}/snowflake_credentials",
        )
    ],
    deployments=[
        FlowDeployment(
            deployment_name="backend_events_for_mozilla",
            parameters={
                "sf_extraction_input": SfExtractionInputs(
                    sql_name="backend_events_for_mozilla",
                    offset_key="collector_tstamp",
                    default_offset="2022-01-01",
                    kwargs={
                        "database_name": "snowplow",
                        "schema_name": "atomic",
                        "table_name": "events",
                    },
                ).dict()
            },
        )  # type: ignore
    ],
)


if __name__ == "__main__":
    t = SfExtractionInputs(
        sql_name="backend_events_for_mozilla",
        offset_key="collector_tstamp",
        default_offset="2022-01-01",
        kwargs={
            "database_name": "snowplow",
            "schema_name": "atomic",
            "table_name": "events",
        },
    )
    run(main(sf_extraction_input=t))  # type: ignore
