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
    SfGcsStage,
    SnowflakeGcsStageSettings,
    get_gcs_stage,
)
from common.deployment.worker import FlowDeployment, FlowSpec
from common.settings import CommonSettings, get_cached_settings
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader
from pendulum.parser import parse as pdm_parse
from prefect import flow, get_run_logger, task
from prefect_gcp.bigquery import bigquery_query
from prefect_snowflake.database import snowflake_multiquery
from shared.async_utils import process_parallel_subflows

load_dotenv
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
) -> tuple:
    """Prefect task to create extraction

    Args:
        sql_path (str): full path for location of extraction template sql
        gcp_creds (MozGcpCredentials): instantiated gcp credentials for connecting to BQ
        interval (tuple): start and end date pair to process
        extra_kwargs (dict, optional): any extra key value pairs to
                                    pass to sql template. Defaults to {}.

    Returns:
        tuple: (rendered sql statement, bucket folder path)
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

    # create the bucket path for files
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
    # render sql statement
    sql_query = render_sql_file("data.sql", sql_path, extra_kwargs)
    # render sql statement wrapped in export logic
    extract_kwargs["sql"] = sql_query
    extraction_sql_stmt = render_sql_string(
        bq_extraction_sql, extra_kwargs=extract_kwargs
    )
    # return the export statement and object folder
    return extraction_sql_stmt, bucket_folder


@task()
def create_load(
    bucket_folder: str,
    sql_path: str,
    sf_stage: SfGcsStage,
    extra_kwargs: dict = {},
) -> str:
    """Create the load statement from sql template.

    Args:
        bucket_folder (str): bucket folder returned from the extract statement create.
        sql_path (str): full path the load sql template
        extra_kwargs (dict, optional): any extra key value pairs to
                                    pass to sql template. Defaults to {}.

    Returns:
        str: snowflake load statement sql text
    """

    # create the snowflake stage uri from stage name and bucket folder
    snowflake_uri = os.path.join(
        sf_stage.stage_name,
        bucket_folder,
    )

    # get table name from parent folder
    path_obj = Path(sql_path)
    table_name = path_obj.stem

    # create the load kwargs
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

    # add any additional kwargs to load kwargs
    load_kwargs.update(extra_kwargs)

    # render and return text
    sql_query = render_sql_file("load.sql", sql_path, load_kwargs)
    return sql_query


@task()
def get_intervals(
    batch_start: str | None = None,
    batch_end: str | None = None,
    include_now: bool = False,
) -> list[tuple]:
    """Get the intervals to process based on start and end date.  This will behave
    like a 'between' statement, which means it will return daily intervals through the
    end date.  Include now means that it will include an interval for today.
    The default will return interval for yesterday.

    Args:
        batch_start (str | None, optional): state date for interval creation.
          Defaults to None. If None then default to yesterday.
        batch_end (str | None, optional): end date for interval creation.
        Defaults to None. If None then default to today.
        include_now (bool, optional): whether to create an interval for today.
        Defaults to False.

    Returns:
        list[tuple]: List of pairs (start_date, end_date), which
        is for >= start_date and < end_date.
    """
    # start date provided then parse to pendulum datetime
    if batch_start:
        start_datetime = pdm_parse(batch_start)
    # else default to current datetime minus 1 day
    else:
        start_datetime = pdm.now(tz="UTC").subtract(days=1)
    # end date provided then parse to pendulum datetime
    if batch_end:
        end_datetime = pdm_parse(batch_end)
    else:
        # else default to current datetime
        end_datetime = pdm.now(tz="UTC")

    # create the list of date objects to process
    period_range = pdm.period(
        start_datetime.start_of("day"), end_datetime.start_of("day"), absolute=True
    )
    # create as pairs to support the proper where clause
    intervals = [
        (x.to_datetime_string(), x.add(days=1).to_datetime_string())
        for x in period_range.range("days")
    ]
    # the period range will include today, so pop if we don't want it
    if not include_now:
        intervals.pop()
    return intervals


@flow(name="data-products.newtab-etl-subflow")
async def interval(
    sql_path: str,
    gcp_creds: MozGcpCredentials,
    sf_creds: MozSnowflakeConnector,
    sf_stage: SfGcsStage,
    interval: tuple,
    extra_kwargs: dict = {},
):
    """Subflow to process an interval for a job folder.

    Args:
        sql_path (str): Path to job templates
        gcp_creds (MozGcpCredentials): instantiated gcp credentials
        sf_creds (MozSnowflakeConnector): instantiated snowflake connector
        interval (tuple): interval key pair to process
        extra_kwargs (dict, optional): any extra key value pairs to
                                    pass to sql templates. Defaults to {}.
    """
    # get extraction sql and folder path
    sql, folder = create_extraction(sql_path, gcp_creds, interval, extra_kwargs)
    # run extraction
    extract = await bigquery_query(sql, gcp_creds)
    # wait for extraction to complete then create load sql
    load_sql = create_load(folder, sql_path, sf_stage, extra_kwargs, wait_for=[extract])  # type: ignore  # noqa: E501
    # split load sql into statements
    queries = load_sql.split(";")[:-1]
    # run load statements
    await snowflake_multiquery(snowflake_connector=sf_creds, queries=queries)


@flow(name="data-products.newtab-etl")
async def main(
    sql_folder: str,
    include_now: bool = False,
    start_date: str | None = None,
    end_date: str | None = None,
    extra_kwargs: dict = {},
):
    """Main flow for executing intervals for a specific job.  This support recursive
    execution.  This means you can pass a specific job like:
    "firefox_new_tab_impressions_daily/firefox_new_tab_daily_disable_rate_by_feed".
    You can also just pass in "firefox_new_tab_impressions_daily" to run all jobs
    in that folder.

    Args:
        sql_folder (str): folder in 'sql' folder to process.  Supports recursive.
        include_now (bool, optional): whether to create an interval for today.
        Defaults to False.
        start_date (str | None, optional): state date for interval creation.
          Defaults to None. If None then default to yesterday.
        end_date (str | None, optional): end date for interval creation.
        Defaults to None. If None then default to today.
        extra_kwargs (dict, optional): any extra key value pairs to
                                    pass to sql templates. Defaults to {}.
    """
    logger = get_run_logger()
    # instantiate creds for the flow
    gcp_creds = MozGcpCredentials()
    sf_creds = MozSnowflakeConnector()
    # gcs stage
    # get the proper snowflake stage from settings
    stg_data = get_cached_settings(SnowflakeGcsStageSettings)
    sf_stage = get_gcs_stage(stg_data.snowflake_gcp_stage_data, "default")
    # get the intervals to process
    intervals = get_intervals(start_date, end_date, include_now)
    # process intervals
    for i in intervals:
        logger.info(f"Processing interval {i}...")
        # list for async interval jobs
        jobs = []
        # for each interval get each job folder
        search_folder = os.path.join(SQL_LOCATION, sql_folder)
        for g in glob(f"{search_folder}/**/data.sql", recursive=True):
            p = Path(g)
            sql_path = str(p.parent)
            table_name = p.parent.stem
            flow_name_date = pdm_parse(i[0]).to_date_string()  # type: ignore
            # process interval subflow for each specific job
            logger.info(f"Submitting {table_name}...")
            jobs.append(
                interval.with_options(flow_run_name=f"{flow_name_date}-{table_name}")(
                    sql_path, gcp_creds, sf_creds, sf_stage, i, extra_kwargs
                )
            )
        await process_parallel_subflows(jobs)
        logger.info(f"Interval {i} completed!")


FLOW_SPEC = FlowSpec(
    flow=main,
    docker_env="base",
    deployments=[
        FlowDeployment(
            name="firefox_new_tab_impressions_daily",
            cron="0 6 * * *",
            parameters={
                "sql_folder": "firefox_new_tab_impressions_daily",
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
                "sql_folder": "firefox_new_tab_impressions_hourly",
                "include_now": True,
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
                "sql_folder": "glean_firefox_new_tab_impressions_daily",
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
                "sql_folder": "glean_firefox_new_tab_impressions_hourly",
                "include_now": True,
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
            name="ads_impressions_hourly",
            cron="0 * * * *",
            parameters={
                "sql_folder": "ads_impressions_hourly",
                "include_now": True,
            },
            job_variables={
                "env": {
                    "DF_CONFIG_SNOWFLAKE_SCHEMA": CS.deployment_type_value(
                        dev="cbeck", staging="staging", main="mozilla"
                    )
                },
            },
            tags=["hourly-sla"],
        ),
    ],
)

if __name__ == "__main__":
    # Set start date to 7 days ago
    start_date = pdm.now(tz="UTC").subtract(days=7).to_date_string()
    # Set end date to today
    end_date = pdm.now(tz="UTC").to_date_string()

    # Run the main flow with backfill for the last 7 days
    run(main("ads_impressions_hourly", start_date=start_date, end_date=end_date))  # type: ignore  # noqa: E501
