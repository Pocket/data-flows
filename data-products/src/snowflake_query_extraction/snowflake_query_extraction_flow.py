"""Query based data extraction from Snowflake Prefect Flow"""
from asyncio import run
from pathlib import Path

from common import get_script_path
from common.databases.snowflake_utils import PktSnowflakeConnector, get_gcs_stage
from common.deployment import FlowDeployment, FlowEnvar, FlowSpec
from common.settings import CommonSettings
from pendulum import now
from prefect import flow, get_run_logger, task
from prefect_snowflake.database import snowflake_query
from pydantic import BaseModel

CS = CommonSettings()  # type: ignore
SFC = PktSnowflakeConnector(
    schema="public", warehouse=f"prefect_wh_{CS.dev_or_production}"
)

CURRENT_OFFSET_SQL = """select coalesce(any_value(state), '{default_offset}') as state
    from query_extraction_state
    where sql_name = '{sql_name}';"""

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
    extraction_sql: str
    persist_state_sql: str


class SfExtractionInputs(BaseModel):
    sql_name: str
    offset_key: str
    default_offset: str
    kwargs: dict = {}

    @property
    def current_offset_sql(self):
        return CURRENT_OFFSET_SQL.format(
            sql_name=self.sql_name,
            offset_key=self.offset_key,
            default_offset=self.default_offset,
            **self.kwargs,
        )

    @property
    def new_offset_sql(self):
        script_path = get_script_path()
        offset_sql_template = Path(
            f"{script_path}/sql/{self.sql_name}/offset.sql"
        ).read_text()
        return offset_sql_template.format(offset_key=self.offset_key, **self.kwargs)


@task()
def create_extraction_job(
    sf_extraction_input: SfExtractionInputs, current_offset: str, new_offset: str
):
    extraction_sql_template = """copy into @{stage_name}/{sql_name}/{year}/{month}/{day}/{hour}/{unix}
    from ({sql})
    header = true
    overwrite = true
    max_file_size = 104857600"""  # noqa: E501

    stage_name = get_gcs_stage()
    ts = now(tz="utc")
    script_path = get_script_path()
    sql_text_template = Path(
        f"{script_path}/sql/{sf_extraction_input.sql_name}/data.sql"
    ).read_text()
    sql_text = sql_text_template.format(
        offset_key=sf_extraction_input.offset_key,
        current_offset=current_offset,
        new_offset=new_offset,
        **sf_extraction_input.kwargs,
    )
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


@flow()
async def snowflake_query_extraction(sf_extraction_input: SfExtractionInputs):
    logger = get_run_logger()
    current_offset = await snowflake_query(
        query=sf_extraction_input.current_offset_sql,
        snowflake_connector=SFC,
    )  # type: ignore
    logger.info(f"Current offset state result: {current_offset[0][0]}...")
    new_offset = await snowflake_query(
        query=sf_extraction_input.new_offset_sql,
        snowflake_connector=SFC,
    )  # type: ignore
    logger.info(f"New offset will be: {new_offset[0][0]}...")
    extraction_job = create_extraction_job(
        sf_extraction_input, current_offset[0][0], new_offset[0][0]
    )
    logger.info("Running extraction...")
    extract = await snowflake_query(
        query=extraction_job.extraction_sql, snowflake_connector=SFC
    )
    logger.info("Applying new state...")
    await snowflake_query(
        query=extraction_job.persist_state_sql,
        snowflake_connector=SFC,
        wait_for=[extract],
    )  # type: ignore


FLOW_SPEC = FlowSpec(
    flow=snowflake_query_extraction,
    docker_env="base",
    secrets=[
        FlowEnvar(
            envar_name="DF_CONFIG_SNOWFLAKE_CREDENTIALS",
            envar_value=f"data-flows/{CS.deployment_type}/snowflake_credentials",
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
            "database_name": "development",
            "schema_name": "braun",
            "table_name": "snowplow_events",
        },
    )
    run(snowflake_query_extraction(sf_extraction_input=t))  # type: ignore
