import os
from datetime import date, datetime, time
from pathlib import Path

import pandas as pd
import pendulum
import requests
from common.databases.snowflake_utils import MozSnowflakeConnector
from common.deployment.worker import FlowDeployment, FlowSpec
from common.settings import CommonSettings, NestedSettings, SecretSettings
from prefect import flow, get_run_logger, task
from prefect_snowflake.database import snowflake_multiquery, snowflake_query_sync
from pydantic import BaseModel
from shared.async_utils import process_parallel_subflows_task

CS = CommonSettings()  # type: ignore


# creating settings objects to leverage new secrets logic
class FreestarCredentials(NestedSettings):
    api_key: str


class FreestarSettings(SecretSettings):
    freestar_credentials: FreestarCredentials


# reusable date format
DATE_FMT = "YYYY-MM-DD"

# Define the output Parquet file template
OUTPUT_PARQUET_PATH = "/tmp/freestar"


def make_file_path(report_date: str) -> str:
    """Produce full file path template based on report_date.

    Args:
        report_date (str): Report date to add to path.

    Returns:
        str: Path string
    """
    return os.path.join(OUTPUT_PARQUET_PATH, report_date, "data_{0}.parquet")


@task(retries=5, retry_delay_seconds=5, task_run_name="extract-api-data-{report_date}")
def extract_freestar_data(report_date: str, api_key: str, record_limit: int):
    """Task for extracting data and loading to file.

    Args:
        report_date (str): Report date to run for.
        api_key (str): API key from settings.
        record_limit (int): Record limit to use.
    """
    logger = get_run_logger()

    # Define the API base URL and endpoint
    api_base_url = "https://analytics.pub.network"
    api_endpoint = "/cubejs-api/v1/load"

    # Define the request payload with initial pagination parameters
    request_payload = {
        "query": {
            "total": True,
            "measures": [
                "NdrPrebid.impressions",
                "NdrPrebid.net_revenue",
                "NdrPrebid.net_cpm",
            ],
            "dimensions": [
                "NdrPrebid.record_date",
                "NdrPrebid.network",
                "NdrPrebid.url",
                "NdrPrebid.utm_campaign",
                "NdrPrebid.utm_content",
                "NdrPrebid.utm_medium",
                "NdrPrebid.utm_source",
                "NdrPrebid.utm_term",
                "NdrPrebid.site_domain",
                "NdrPrebid.ad_unit",
                "NdrPrebid.size",
                "NdrPrebid.country_code",
                "NdrPrebid.device_type",
                "NdrPrebid.device_os",
                "NdrPrebid.browser",
                "NdrPrebid.integration_partner",
            ],
            "timeDimensions": [
                {
                    "dimension": "NdrPrebid.record_date",
                    "dateRange": [report_date, report_date],
                }
            ],
            "limit": record_limit,
            "offset": 0,  # Start with an offset of 0
        }
    }
    # Set up the headers with the API token
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    page_number = 1  # Initialize the page number tracker
    total_count = 0  # Initialize the total count tracker

    # create directory
    file_path = make_file_path(report_date)
    file_path_obj = Path(file_path)
    file_path_obj.parent.mkdir(parents=True, exist_ok=True)

    while True:
        # Send the POST request to the API with the current pagination parameters
        response = requests.post(
            api_base_url + api_endpoint, json=request_payload, headers=headers
        )
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Parse the JSON response
        response_json = response.json()
        logger.info(
            f"Retrieved {len(response_json['data'])} records for page {page_number}."
        )

        # Remove the 'NdrPrebid' prefix from keys in the JSON response
        for item in response_json["data"]:
            for key in list(item.keys()):
                if key.startswith("NdrPrebid."):
                    new_key = key[len("NdrPrebid.") :]
                    item[new_key] = item.pop(key)

        # Write to file
        data = response_json["data"]
        record_count = len(data)
        df = pd.DataFrame(data)
        df.to_parquet(file_path.format(page_number))
        total_count += record_count

        # Check if there are more records to retrieve
        if record_count < record_limit:
            break  # Stop if there are no more records

        # Increment the offset for the next page
        request_payload["query"]["offset"] += record_limit
        page_number += 1

    logger.info(f"Total records retrieved: {total_count}")


# SQL for getting dates currently in Snowflake
ACTUAL_DATES = """select distinct RECORD_DATE
    from freestar.freestar_daily_extracts
    where RECORD_DATE >= %(start_date)s
    and RECORD_DATE <= %(end_date)s"""

TABLE_SCHEMA = """
    record_date DATE,
    network STRING,
    url STRING,
    utm_campaign STRING,
    utm_content STRING,
    utm_medium STRING,
    utm_source STRING,
    utm_term STRING,
    site_domain STRING,
    ad_unit STRING,
    size STRING,
    country_code STRING,
    device_type STRING,
    device_os STRING,
    browser STRING,
    integration_partner STRING,
    impressions INTEGER,
    net_revenue INTEGER,
    net_cpm FLOAT
"""

# Define table to load
SNOWFLAKE_TABLE = "freestar_daily_extracts"

# Define SQL statements
CREATE_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS freestar;
"""

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS freestar.{SNOWFLAKE_TABLE} (
    {TABLE_SCHEMA}
);
"""

FORMAT_FILE_SQL = f"""
CREATE OR REPLACE FILE FORMAT freestar.{SNOWFLAKE_TABLE}_format
TYPE = parquet;
"""

CREATE_STAGE_SQL = f"""
CREATE OR REPLACE STAGE freestar.{SNOWFLAKE_TABLE}_stage
FILE_FORMAT = freestar.{SNOWFLAKE_TABLE}_format;
"""


@flow(
    name="freestar-report-flow.ingest-freestar-data-subflow",
    flow_run_name="freestar-ingestion-{report_date}",
)
async def ingest_freestar_data_subflow(
    report_date: str,
    sf_connector: MozSnowflakeConnector,
    api_key: str,
    record_limit: int,
):
    """Subflow for loading parquet files into Snowflake.

    Args:
        report_date (str): Report date to run for.
        sf_connector (MozSnowflakeConnector): Connector block to use for tasks.
        api_key (str): API key from settings.
        record_limit (int): Record limit to use.
    """
    tmp_table_name = f"{SNOWFLAKE_TABLE}_{report_date.replace('-', '')}_tmp"

    file_path = make_file_path(report_date)

    put_parquet_sql = f"""
    PUT file://{file_path.format('*')} @freestar.{SNOWFLAKE_TABLE}_stage/{report_date}
    """

    create_temp_table = f"""
    CREATE TEMPORARY TABLE freestar.{tmp_table_name} (
        {TABLE_SCHEMA}
    );
    """

    load_sql = f"""
    COPY INTO freestar.{tmp_table_name}
        FROM (SELECT $1:record_date::DATE,
                    $1:network::STRING,
                    $1:url::STRING,
                    $1:utm_campaign::STRING,
                    $1:utm_content::STRING,
                    $1:utm_medium::STRING,
                    $1:utm_source::STRING,
                    $1:utm_term::STRING,
                    $1:site_domain::STRING,
                    $1:ad_unit::STRING,
                    $1:size::STRING,
                    $1:country_code::STRING,
                    $1:device_type::STRING,
                    $1:device_os::STRING,
                    $1:browser::STRING,
                    $1:integration_partner::STRING,
                    $1:impressions::INTEGER,
                    $1:net_revenue::INTEGER,
                    $1:net_cpm::FLOAT
            FROM @freestar.{SNOWFLAKE_TABLE}_stage/{report_date}
            );
    """

    begin_transaction = """
    BEGIN TRANSACTION;
    """

    delete_sql = f"""
    DELETE FROM freestar.{SNOWFLAKE_TABLE}
        WHERE record_date IN (SELECT record_date FROM freestar.{tmp_table_name});
    """

    insert_sql = f"""
    INSERT INTO freestar.{SNOWFLAKE_TABLE}
        SELECT * FROM freestar.{tmp_table_name};
    """

    delete_tmp_table_sql = f"""
    DROP TABLE freestar.{tmp_table_name};
    """

    sfc = sf_connector
    eft = extract_freestar_data.with_options(
        task_run_name=f"load-api-data-files-{report_date}"
    )(report_date, api_key, record_limit)
    put_parquet = await snowflake_query_sync(
        query=put_parquet_sql,
        snowflake_connector=sfc,
        wait_for=[eft],  # type: ignore
    )
    await snowflake_multiquery.with_options(
        task_run_name=f"load-api-data-to-table-{report_date}"
    )(
        queries=[
            create_temp_table,
            load_sql,
            begin_transaction,
            delete_sql,
            insert_sql,
            delete_tmp_table_sql,
        ],
        snowflake_connector=sfc,
        wait_for=[put_parquet],  # type: ignore
        as_transaction=True,
    )


class FlowDateInputs(BaseModel):
    """Model to represent flow inputs.
    This also make fields optional to enable default logic.
    """

    start_date: date | None = None
    end_date: date | None = None
    overwrite: bool = False
    record_limit: int = 50000


# Define the Prefect flow
@flow(name="freestar-revenue-reporting.freestar-report-flow")
async def freestar_report_flow(dates: FlowDateInputs = FlowDateInputs()):
    """Main flow that will bootstrap Snowflake objects as needed.
    This will also do the necessary evaluation of dates to determine
    what needs to be processed.  This include handling an overwrite and backfill.

    Args:
        dates (FlowDateInputs, optional): Flow inputs. Defaults to FlowDateInputs().
    """

    # create connector to pass on
    sfc = MozSnowflakeConnector()

    async def get_dates(
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[str]:
        """Nested function handle date evaluation

        Args:
            start_date (date | None, optional): Explicit value to use. Defaults to None.
            end_date (date | None, optional): Explicit value to use. Defaults to None.

        Returns:
            list: list of date strings.
        """

        # default date logic for regular runs
        if not start_date:
            start_date = pendulum.now().subtract(days=14)
        else:
            start_date = pendulum.instance(datetime.combine(start_date, time(0, 0, 0)))

        if not end_date:
            end_date = pendulum.now().subtract(days=1)
        else:
            end_date = pendulum.instance(datetime.combine(end_date, time(0, 0, 0)))

        # logic to determine base set if dates to process
        dt_period = end_date - start_date
        dt_period.in_days()  # type: ignore

        # logic to produce final dates to process using dates already
        # processed if enabled
        expected_days = [d.date() for d in dt_period]  # type: ignore
        dates_to_process = sorted(set(expected_days))
        if not dates.overwrite:
            actual_dates = await snowflake_query_sync(
                query=ACTUAL_DATES,
                snowflake_connector=sfc,
                params={"start_date": start_date, "end_date": end_date},
            )  # type: ignore

            actual_days = [x[0] for x in actual_dates]
            dates_to_process = sorted(set(expected_days).difference(set(actual_days)))

        return [i.format(DATE_FMT) for i in list(dates_to_process)]

    # bootstrap tables
    create_schema = await snowflake_query_sync(
        query=CREATE_SCHEMA_SQL,
        snowflake_connector=sfc,
    )
    create = await snowflake_query_sync(
        query=CREATE_TABLE_SQL,
        snowflake_connector=sfc,
        wait_for=[create_schema],  # type: ignore
    )
    format_file = await snowflake_query_sync(
        query=FORMAT_FILE_SQL,
        snowflake_connector=sfc,
        wait_for=[create],  # type: ignore
    )
    create_stage = await snowflake_query_sync(
        query=CREATE_STAGE_SQL,
        snowflake_connector=sfc,
        wait_for=[format_file],  # type: ignore
    )

    # Fetch credentials
    freestar_creds = FreestarSettings()  # type: ignore

    # create and submit subflows
    jobs = [
        ingest_freestar_data_subflow(
            j, sfc, freestar_creds.freestar_credentials.api_key, dates.record_limit
        )
        for j in await get_dates(start_date=dates.start_date, end_date=dates.end_date)
    ]
    await process_parallel_subflows_task(jobs, wait_for=[create_stage])  # type: ignore


FLOW_SPEC = FlowSpec(
    flow=freestar_report_flow,
    docker_env="base",
    deployments=[
        FlowDeployment(
            name="freestar_extraction",
            # Running at 10 a.m. UTC to ensure it runs after midnight
            # since we are pulling from Freestar's "Yesterday"
            cron="0 10 * * *",
        ),
    ],
)

if __name__ == "__main__":
    from asyncio import run

    run(freestar_report_flow())
