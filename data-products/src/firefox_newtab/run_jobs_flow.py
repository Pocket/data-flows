"""Prefect flow to extract and load BigQuery New Tab Data into Snowflake"""

import os
from asyncio import run
from glob import glob
from pathlib import Path

import pendulum as pdm
from common import get_script_path
from common.cloud.gcp_utils import MozGcpCredentials
from common.databases.snowflake_utils import (
    MozSnowflakeConnector,
    SnowflakeGcsStageSettings,
    get_gcs_stage,
)
from common.deployment.worker import FlowDeployment, FlowSpec
from common.settings import CommonSettings, get_cached_settings
from jinja2 import Environment, FileSystemLoader
from pendulum.parser import parse as pdm_parse
from prefect import flow, get_run_logger, task
from prefect_gcp.bigquery import bigquery_query
from prefect_snowflake.database import snowflake_multiquery

CS = CommonSettings()  # type: ignore

# location of sql is relative to where the flow code is
SQL_LOCATION = os.path.join(get_script_path(), "sql")


# helpers for rendering jinja2 templating in sql files
def render_sql_string(sql_string: str, extra_kwargs: dict = {}) -> str:
    """Helper method for rendering a jinj2 sql string
    using job kwargs plus optional additional kwargs.

    Args:
        sql_string (str): SQL text with jijna2 template logic.
        extra_kwargs (dict): Optional kwargs to pass into template on top of
        the job_kwargs.

    Returns:
        SqlStmt: SqlStmt object with sql text and db engine.
    """
    environment = Environment()
    j2_env = environment
    template = j2_env.from_string(sql_string)
    return template.render(**extra_kwargs)


def render_sql_file(
    sql_file_name: str, sql_folder: str, extra_kwargs: dict = {}
) -> str:
    """Helper method for rendering a jinja2 sql file using
    job kwargs plus optional additional kwargs.

    Args:
        sql_file (str): File name containing SQL text with jijna2 template logic.
        extra_kwargs (dict): Optional kwargs to pass into template on top of
        the job_kwargs.

    Returns:
        SqlStmt: SqlStmt object with sql text and db engine.
    """
    # create path string for location of helpers.j2
    extras_path = os.path.join(SQL_LOCATION, "extras")

    environment = Environment(
        loader=FileSystemLoader([sql_folder, extras_path]),
    )
    j2_env = environment
    template = j2_env.get_template(sql_file_name)
    return template.render(**extra_kwargs)


@task()
def create_extraction(
    sql_path: str,
    gcp_creds: MozGcpCredentials,
    interval: tuple,
    extra_kwargs: dict = {},
):
    """Prefect task to create extraction

    Args:
        sql_path (str): _description_
        gcp_creds (MozGcpCredentials): _description_
        interval (tuple): _description_
        extra_kwargs (dict, optional): _description_. Defaults to {}.

    Returns:
        _type_: _description_
    """
    batch_start, batch_end = interval
    extra_kwargs["batch_start"] = batch_start
    extra_kwargs["batch_end"] = batch_end
    bq_extraction_sql = """EXPORT DATA OPTIONS(
          uri='gs://{{ gcs_uri }}',
          format='PARQUET',
          compression='SNAPPY',
          overwrite=true) AS
        {{ sql }}"""  # noqa: E501

    relative_path = sql_path.replace(SQL_LOCATION + "/", "")

    batch_folder = pdm_parse(batch_start).to_date_string()
    bucket_folder = os.path.join(
        relative_path,
        batch_folder,
        str(pdm.now(tz="UTC").int_timestamp),
    )

    extract_kwargs = {
        "gcs_uri": os.path.join(gcp_creds.staging_bucket, bucket_folder, "data*.parq")
    }
    sql_query = render_sql_file("data.sql", sql_path, extra_kwargs)
    extract_kwargs["sql"] = sql_query
    extraction_sql_stmt = render_sql_string(
        bq_extraction_sql, extra_kwargs=extract_kwargs
    )
    return extraction_sql_stmt, bucket_folder


@task()
def create_load(
    bucket_folder: str,
    sql_path: str,
    extra_kwargs: dict = {},
):
    """_summary_

    Args:
        bucket_folder (str): _description_
        sql_path (str): _description_
        extra_kwargs (dict, optional): _description_. Defaults to {}.

    Returns:
        _type_: _description_
    """
    stg_data = get_cached_settings(SnowflakeGcsStageSettings)
    stage_location = get_gcs_stage(stg_data.snowflake_gcp_stage_data, "default")

    snowflake_uri = os.path.join(
        stage_location.stage_name,
        bucket_folder,
    )
    path_obj = Path(sql_path)
    table_name = path_obj.stem
    load_kwargs = {
        "snowflake_stage_uri": snowflake_uri,
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
        "table_name": table_name,
    }
    load_kwargs.update(extra_kwargs)
    sql_query = render_sql_file("load.sql", sql_path, load_kwargs)
    return sql_query


@task()
def get_intervals(
    batch_start: str | None = None,
    batch_end: str | None = None,
    include_now: bool = False,
):
    """_summary_

    Args:
        batch_start (str | None, optional): _description_. Defaults to None.
        batch_end (str | None, optional): _description_. Defaults to None.
        include_now (bool, optional): _description_. Defaults to False.

    Returns:
        _type_: _description_
    """
    if batch_start:
        start_datetime = pdm_parse(batch_start)
    else:
        start_datetime = pdm.now(tz="UTC").subtract(days=1)
    if batch_end:
        end_datetime = pdm_parse(batch_end)
    else:
        end_datetime = pdm.now(tz="UTC")
    period_range = pdm.period(
        start_datetime.start_of("day"), end_datetime.start_of("day"), absolute=True
    )
    intervals = [
        (x.to_datetime_string(), x.add(days=1).to_datetime_string())
        for x in period_range.range("days")
    ]
    if not include_now:
        intervals.pop()
    return intervals


@flow(name="data-products.newtab-etl-subflow")
async def interval(
    sql_path: str,
    gcp_creds: MozGcpCredentials,
    sf_creds: MozSnowflakeConnector,
    interval: tuple,
    extra_kwargs: dict = {},
):
    """_summary_

    Args:
        sql_path (str): _description_
        gcp_creds (MozGcpCredentials): _description_
        sf_creds (MozSnowflakeConnector): _description_
        interval (tuple): _description_
        extra_kwargs (dict, optional): _description_. Defaults to {}.
    """
    sql, folder = create_extraction(sql_path, gcp_creds, interval, extra_kwargs)
    extract = await bigquery_query(sql, gcp_creds)
    load_sql = create_load(folder, sql_path, extra_kwargs, wait_for=[extract])  # type: ignore
    queries = load_sql.split(";")[:-1]
    await snowflake_multiquery(snowflake_connector=sf_creds, queries=queries)


@flow(name="data-products.newtab-etl")
async def main(
    sql_folder: str,
    include_now: bool = False,
    start_date: str | None = None,
    end_date: str | None = None,
    extra_kwargs: dict = {},
):
    """_summary_

    Args:
        sql_folder (str): _description_
        include_now (bool, optional): _description_. Defaults to False.
        start_date (str | None, optional): _description_. Defaults to None.
        end_date (str | None, optional): _description_. Defaults to None.
        extra_kwargs (dict, optional): _description_. Defaults to {}.
    """
    gcp_creds = MozGcpCredentials()
    sf_creds = MozSnowflakeConnector()
    intervals = get_intervals(start_date, end_date, include_now)
    for i in intervals:
        search_folder = os.path.join(SQL_LOCATION, sql_folder)
        for g in glob(f"{search_folder}/**/data.sql", recursive=True):
            p = Path(g)
            sql_path = str(p.parent)
            await interval(sql_path, gcp_creds, sf_creds, i, extra_kwargs)


FLOW_SPEC = FlowSpec(
    flow=main,
    docker_env="base",
    deployments=[
        FlowDeployment(
            name="firefox_new_tab_impressions_daily",
            cron="0 6 * * *",
            parameters={
                "etl_input": SqlEtlJob(
                    sql_folder_name="firefox_new_tab_impressions_daily"
                ).dict()  # type: ignore
            },
            job_variables={
                "env": {
                    "DF_CONFIG_SNOWFLAKE_SCHEMA": CS.deployment_type_value(
                        dev="braun", staging="staging", main="mozilla"
                    )
                },
            },
            tags=["daily-sla"],
        ),
        FlowDeployment(
            name="firefox_new_tab_impressions_hourly",
            cron="0 * * * *",
            parameters={
                "etl_input": SqlEtlJob(
                    sql_folder_name="firefox_new_tab_impressions_hourly"
                ).dict()  # type: ignore
            },
            job_variables={
                "env": {
                    "DF_CONFIG_SNOWFLAKE_SCHEMA": CS.deployment_type_value(
                        dev="braun", staging="staging", main="mozilla"
                    )
                },
            },
            tags=["hourly-sla"],
        ),
        FlowDeployment(
            name="glean_firefox_new_tab_impressions_daily",
            cron="0 6 * * *",
            parameters={
                "etl_input": SqlEtlJob(
                    sql_folder_name="glean_firefox_new_tab_impressions_daily"
                ).dict()  # type: ignore
            },
            job_variables={
                "env": {
                    "DF_CONFIG_SNOWFLAKE_SCHEMA": CS.deployment_type_value(
                        dev="braun", staging="staging", main="mozilla"
                    )
                },
            },
            tags=["daily-sla"],
        ),
        FlowDeployment(
            name="glean_firefox_new_tab_impressions_hourly",
            cron="0 * * * *",
            parameters={
                "etl_input": SqlEtlJob(
                    sql_folder_name="glean_firefox_new_tab_impressions_hourly"
                ).dict()  # type: ignore
            },
            job_variables={
                "env": {
                    "DF_CONFIG_SNOWFLAKE_SCHEMA": CS.deployment_type_value(
                        dev="braun", staging="staging", main="mozilla"
                    )
                },
            },
            tags=["hourly-sla"],
        ),
    ],
)

if __name__ == "__main__":
    run(main("firefox_new_tab_impressions_daily/firefox_new_tab_daily_disable_rate_by_feed"))  # type: ignore  # noqa: E501
