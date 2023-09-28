import json
import os
import time
from pathlib import Path

import pandas as pd
import requests
from common.databases.snowflake_utils import PktSnowflakeConnector
from common.deployment import FlowDeployment, FlowEnvar, FlowSpec
from common.settings import CommonSettings
from prefect import flow, get_run_logger, task
from prefect.server.schemas.schedules import CronSchedule
from prefect_snowflake.database import snowflake_multiquery, snowflake_query_sync

CS = CommonSettings()  # type: ignore

# Define the output Parquet file template
output_parquet_path = "/tmp/freestar"
output_parquet_filename = os.path.join(output_parquet_path, "data_{0}.parquet")


@task(retries=3, retry_delay_seconds=5)
def extract_freestar_data():
    logger = get_run_logger()
    # Define the API base URL and endpoint
    API_BASE_URL = "https://analytics.pub.network"
    API_ENDPOINT = "/cubejs-api/v1/load"
    FREESTAR_API_KEY = json.loads(os.environ["FREESTAR_CREDENTIALS"])["api_key"]

    # Retrieve 2,000 records at a time to accomodate 5 second timeout
    record_limit = 2000

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
                    "dateRange": "Yesterday",  # for backfill use "dateRange": ["2023-09-01", "2023-09-08"]
                }
            ],
            "limit": record_limit,  # Retrieve 2,000 records at a time to accomodate 5 second timeout
            "offset": 0,  # Start with an offset of 0
        }
    }

    # Set up the headers with the API token
    headers = {
        "Authorization": f"Bearer {FREESTAR_API_KEY}",
        "Content-Type": "application/json",
    }

    page_number = 1  # Initialize the page number tracker
    total_count = 0  # Initialize the total count tracker

    # create directory
    Path(output_parquet_path).mkdir(parents=True, exist_ok=True)

    while True:
        # Send the POST request to the API with the current pagination parameters
        response = requests.post(
            API_BASE_URL + API_ENDPOINT, json=request_payload, headers=headers
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
        df.to_parquet(output_parquet_filename.format(page_number))
        total_count += record_count

        # Check if there are more records to retrieve
        if record_count < record_limit:
            break  # Stop if there are no more records

        # Increment the offset for the next page
        request_payload["query"]["offset"] += record_limit
        page_number += 1

        # Add a 5-second delay before fetching the next page to accomodate
        # Freestar's API limits
        time.sleep(5)

    logger.info(f"Total records retrieved: {total_count}")


table_schema = """
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
snowflake_table = "freestar_daily_extracts"

# Define SQL statements
create_schema_sql = """
CREATE SCHEMA IF NOT EXISTS freestar;
"""

create_table_sql = f"""
CREATE TABLE IF NOT EXISTS freestar.{snowflake_table} (
    {table_schema}
);
"""

format_file_sql = f"""
CREATE OR REPLACE FILE FORMAT freestar.{snowflake_table}_format
TYPE = parquet;
"""

create_stage_sql = f"""
CREATE OR REPLACE STAGE freestar.{snowflake_table}_stage
FILE_FORMAT = {snowflake_table}_format;
"""

put_parquet_sql = f"""
PUT file://{output_parquet_filename.format('*')} @{snowflake_table}_stage
"""

create_temp_table = f"""
CREATE TEMPORARY TABLE freestar.{snowflake_table}_tmp (
    {table_schema}
);
"""

load_sql = f"""
COPY INTO freestar.{snowflake_table}_tmp
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
        FROM @freestar.{snowflake_table}_stage
        );
"""

begin_transaction = """
BEGIN TRANSACTION;
"""

delete_sql = f"""
DELETE FROM freestar.{snowflake_table}
    WHERE record_date IN (SELECT record_date FROM freestar.{snowflake_table}_tmp);
"""

insert_sql = f"""
INSERT INTO freestar.{snowflake_table}
    SELECT * FROM freestar.{snowflake_table}_tmp;
"""


# Define the Prefect flow
@flow(name="Freestar Report Flow")
async def freestar_report_flow():
    eft = extract_freestar_data()
    create_schema = await snowflake_query_sync(
        query=create_schema_sql,
        snowflake_connector=PktSnowflakeConnector(),
        wait_for=[eft],
    )  # type: ignore
    create = await snowflake_query_sync(
        query=create_table_sql,
        snowflake_connector=PktSnowflakeConnector(),
        wait_for=[create_schema],
    )  # type: ignore
    format_file = await snowflake_query_sync(
        query=format_file_sql,
        snowflake_connector=PktSnowflakeConnector(),
        wait_for=[create],
    )  # type: ignore
    create_stage = await snowflake_query_sync(
        query=create_stage_sql,
        snowflake_connector=PktSnowflakeConnector(),
        wait_for=[format_file],
    )  # type: ignore
    put_parquet = await snowflake_query_sync(
        query=put_parquet_sql,
        snowflake_connector=PktSnowflakeConnector(),
        wait_for=[create_stage],
    )  # type: ignore
    await snowflake_multiquery(
        queries=[
            create_temp_table,
            load_sql,
            begin_transaction,
            delete_sql,
            insert_sql,
        ],
        snowflake_connector=PktSnowflakeConnector(),
        wait_for=[put_parquet],
        as_transaction=True,
    )  # type: ignore


FLOW_SPEC = FlowSpec(
    flow=freestar_report_flow,
    docker_env="base",
    secrets=[
        FlowEnvar(
            envar_name="DF_CONFIG_SNOWFLAKE_CREDENTIALS",
            envar_value=f"data-flows/{CS.deployment_type}/snowflake-credentials",
        ),
        FlowEnvar(
            envar_name="FREESTAR_CREDENTIALS",
            envar_value=f"data-flows/{CS.deployment_type}/freestar-credentials",
        ),
    ],
    deployments=[
        FlowDeployment(
            deployment_name="freestar_extraction",
            # Running at 10 a.m. UTC to ensure it runs after midnight
            # since we are pulling from Freestar's "Yesterday"
            schedule=CronSchedule(cron="0 10 * * *"),
        ),
    ],
)

if __name__ == "__main__":
    from asyncio import run

    run(freestar_report_flow())  # type: ignore
