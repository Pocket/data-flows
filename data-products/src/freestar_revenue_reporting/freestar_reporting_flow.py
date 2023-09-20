import requests
import json
import os
import time
import shutil

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

# Define a subdirectory to store JSON objects
subdirectory = "json_data"

# Initialize an empty list to store the data from each JSON file
combined_data = []

# Define the output JSON file to save the combined data
output_json_filename = "data_combined.json"
output_json_file = os.path.join(subdirectory, output_json_filename)

# Define the output Parquet file
output_parquet_filename = "data_combined.parquet"

@task(retries=3, retry_delay_seconds=5)
def extract_freestar_data():
    
    # Define the API base URL and endpoint
    API_BASE_URL = "https://analytics.pub.network"
    API_ENDPOINT = "/cubejs-api/v1/load"
    FREESTAR_API_KEY = json.loads(os.environ["FREESTAR_CREDENTIALS"])['api_key']

    # Define the request payload with initial pagination parameters
    request_payload = {
        "query": {
            "total": True,
            "measures": [
                "NdrPrebid.impressions",
                "NdrPrebid.net_revenue",
                "NdrPrebid.net_cpm"
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
                "NdrPrebid.integration_partner"
            ],
            "timeDimensions": [
                {
                    "dimension": "NdrPrebid.record_date",
                    "dateRange": "Yesterday" # for backfill use "dateRange": ["2023-09-01", "2023-09-08"]
                }
            ],
            "limit": 2000,  # Retrieve 2,000 records at a time to accomodate 5 second timeout
            "offset": 0  # Start with an offset of 0
        }
    }

    # Set up the headers with the API token
    headers = {
        "Authorization": f"Bearer {FREESTAR_API_KEY}",
        "Content-Type": "application/json"
    }

    # Create the subdirectory (if it exists, remove it first to ensure it's empty)
    if os.path.exists(subdirectory):
        shutil.rmtree(subdirectory)
        os.makedirs(subdirectory)

    # Define a function to save data to a file
    def save_data_to_file(data, filename):
        filepath = os.path.join(subdirectory, filename)
        with open(filepath, "w") as output_file:
            json.dump(data, output_file, indent=4)

    # Define a function to save combined data to a Parquet file
    def save_data_to_parquet(data, filename):
        filepath = os.path.join(subdirectory, filename)
        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, filepath)

    page_number = 1  # Initialize the page number

    while True:
        try:
            # Send the POST request to the API with the current pagination parameters
            response = requests.post(API_BASE_URL + API_ENDPOINT, json=request_payload, headers=headers)
            response.raise_for_status()  # Raise an exception for HTTP errors

            # Parse the JSON response
            response_json = response.json()

            # Remove the 'NdrPrebid' prefix from keys in the JSON response
            for item in response_json['data']:
                for key in list(item.keys()):
                    if key.startswith('NdrPrebid.'):
                        new_key = key[len('NdrPrebid.'):]
                        item[new_key] = item.pop(key)

            # Save the JSON response to a separate file with a page number
            filename = f"api_response_page_{page_number}.json"
            save_data_to_file(response_json, filename)
            print(f"Page {page_number} saved to {filename}")

            # Check if there are more records to retrieve
            if len(response_json['data']) < 2000:
                break  # Stop if there are no more records

            # Increment the offset for the next page
            request_payload["query"]["offset"] += 2000
            page_number += 1

            # Add a 5-second delay before fetching the next page
            time.sleep(5)

        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON response: {e}")

    # Combine paginated files

    # Loop through all JSON files in the directory
    for filename in os.listdir(subdirectory):
        if filename.endswith(".json"):
            file_path = os.path.join(subdirectory, filename)
            with open(file_path, "r") as json_file:
                try:
                    data = json.load(json_file)["data"]
                    combined_data.extend(data)  # Extend the list with data blocks
                except json.JSONDecodeError:
                    print(f"Error reading JSON from {filename}")
    
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
PUT file://{subdirectory}/{output_parquet_filename} @{snowflake_table}_stage
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
        FROM @{snowflake_table}_stage/data_combined.parquet
        );
"""

# Define the Prefect flow
@flow(name="Freestar Report Flow")
async def freestar_report_flow():
    extract_freestar_data()
    await snowflake_query_sync(
        query=create_table_sql,
        snowflake_connector=PktSnowflakeConnector()
    )
    await snowflake_query_sync(
        query=format_file_sql,
        snowflake_connector=PktSnowflakeConnector()
    )
    await snowflake_query_sync(
        query=create_stage_sql,
        snowflake_connector=PktSnowflakeConnector()
    )
    await snowflake_query_sync(
        query=put_parquet_sql,
        snowflake_connector=PktSnowflakeConnector()
    )
    await snowflake_query_sync(
        query=load_sql,
        snowflake_connector=PktSnowflakeConnector()
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
        )
    ],
    deployments=[
        FlowDeployment(
            deployment_name="freestar_extraction",
            schedule=CronSchedule(cron="0 10 * * *"),
        ),
    ],
)

if __name__ == "__main__":
    from asyncio import run
    run(freestar_report_flow())
