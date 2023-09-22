import requests
import json
import os
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from prefect import flow, task
from prefect.server.schemas.schedules import CronSchedule
from prefect_snowflake.database import snowflake_query_sync

from common.databases.snowflake_utils import PktSnowflakeConnector
from common.deployment import FlowDeployment, FlowEnvar, FlowSpec
from common.settings import CommonSettings

CS = CommonSettings()  # type: ignore

# Initialize an empty list to store the data from each JSON file
combined_data = []

# Define the output Parquet file
output_parquet_filename = "data_combined.parquet"


@task(retries=3, retry_delay_seconds=5)
def extract_freestar_data():
    # Define the API base URL and endpoint
    API_BASE_URL = "https://analytics.pub.network"
    API_ENDPOINT = "/cubejs-api/v1/load"
    FREESTAR_API_KEY = json.loads(os.environ["FREESTAR_CREDENTIALS"])["api_key"]

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
                    "dateRange": ["Yesterday"],  # for backfill use "dateRange": ["2023-09-01", "2023-09-08"]
                }
            ],
            "limit": 2000,  # Retrieve 2,000 records at a time to accomodate 5 second timeout
            "offset": 0,  # Start with an offset of 0
        }
    }

    # Set up the headers with the API token
    headers = {
        "Authorization": f"Bearer {FREESTAR_API_KEY}",
        "Content-Type": "application/json",
    }

    # Define a function to save combined data to a Parquet file
    def save_data_to_parquet(data, filename):
        df = pd.DataFrame(data)
        df.to_parquet(filename)

    page_number = 1  # Initialize the page number

    while True:
        # Send the POST request to the API with the current pagination parameters
        response = requests.post(
            API_BASE_URL + API_ENDPOINT, json=request_payload, headers=headers
        )
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Parse the JSON response
        response_json = response.json()
        print(f"Retrieved {len(response_json['data'])} records for page {page_number}.")

        # Remove the 'NdrPrebid' prefix from keys in the JSON response
        for item in response_json["data"]:
            for key in list(item.keys()):
                if key.startswith("NdrPrebid."):
                    new_key = key[len("NdrPrebid.") :]
                    item[new_key] = item.pop(key)

        # Append the processed data to the combined_data list
        combined_data.extend(response_json["data"])

        # Check if there are more records to retrieve
        if len(response_json["data"]) < 2000:
            break  # Stop if there are no more records

        # Increment the offset for the next page
        request_payload["query"]["offset"] += 2000
        page_number += 1

        # Add a 5-second delay before fetching the next page
        time.sleep(5)

    print(f"Total records retrieved: {len(combined_data)}")

    # Convert the combined data to Parquet format and save it
    output_parquet_filename = "data_combined.parquet"
    save_data_to_parquet(combined_data, output_parquet_filename)
    print("Combined data saved to", output_parquet_filename)


# Define table to load
snowflake_table = "freestar_daily_extracts"

# Define SQL statements
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {snowflake_table} (
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
);
"""

format_file_sql = f"""
CREATE OR REPLACE FILE FORMAT {snowflake_table}_format
TYPE = parquet;
"""

create_stage_sql = f"""
CREATE OR REPLACE STAGE {snowflake_table}_stage
FILE_FORMAT = {snowflake_table}_format;
"""

put_parquet_sql = f"""
PUT file://{output_parquet_filename} @{snowflake_table}_stage
"""

load_sql = f"""
COPY INTO {snowflake_table}
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
        FROM @{snowflake_table}_stage/{output_parquet_filename}
        );
"""


# Define the Prefect flow
@flow(name="Freestar Report Flow")
async def freestar_report_flow():
    extract_freestar_data()
    await snowflake_query_sync(
        query=create_table_sql, snowflake_connector=PktSnowflakeConnector()
    )
    await snowflake_query_sync(
        query=format_file_sql, snowflake_connector=PktSnowflakeConnector()
    )
    await snowflake_query_sync(
        query=create_stage_sql, snowflake_connector=PktSnowflakeConnector()
    )
    await snowflake_query_sync(
        query=put_parquet_sql, snowflake_connector=PktSnowflakeConnector()
    )
    await snowflake_query_sync(
        query=load_sql, snowflake_connector=PktSnowflakeConnector()
    )


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

    run(freestar_report_flow())
