import json
import os

from cloud.gcp_utils import PktBigQueryWarehouse, PktGcpCredentials
from databases.snowflake_utils import PktSnowflakeConnector

# from pendulum import now as pd_now
from prefect import flow, task
from prefect_gcp.bigquery import bigquery_query
from prefect_snowflake.database import snowflake_query

# from pathlib import Path


SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
MOZ_DATA_PROJECT = os.getenv("MOZ_DATA_PROJECT", "moz-fx-data-shared-prod")


@task()
def create_bq_schema():
    with PktBigQueryWarehouse() as w:
        result = w.execute("CREATE SCHEMA IF NOT EXISTS mozilla")
        return result


@task()
def bq_extract(input):
    print(input)
    return input


@flow()
def bq_to_snowflake(subflow_set_def: dict):
    # ds = create_bq_schema()
    stg = bigquery_query(
        gcp_credentials=PktGcpCredentials(),
        # query=Path(
        #     os.path.join(SCRIPT_PATH, subflow_set_def["source_sql_path"])
        # ).read_text(),
        query="""select 1 from `moz-fx-data-shared-prod.activity_stream_live.sessions_v1`
        where submission_timestamp > '2023-05-11'
        limit 1;""",  # noqa: E501
        # dataset=f'{subflow_set_def["schema_name"]}',
        # table=f'{subflow_set_def["seq_id"]}_{int(pd_now(tz="utc").timestamp())}',
        # job_config={
        #     "create_disposition": "CREATE_IF_NEEDED",
        #     "write_disposition": "WRITE_TRUNCATE",
        # },
        #  wait_for=[ds] # type: ignore
    )
    ex = bq_extract(stg)
    snowflake_query(
        snowflake_connector=PktSnowflakeConnector(),
        query="select 1;",
        wait_for=[ex],  # type: ignore
    )


def subflow_factory(group_id: str):
    with open(os.path.join(SCRIPT_PATH, "config/bq_to_snowflake.json")) as f:
        groups = json.load(f)
    group = groups[group_id]
    for s in group["subflow_set_defs"]:
        bq_to_snowflake(s)
