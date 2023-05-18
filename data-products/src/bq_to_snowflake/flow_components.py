import asyncio
import json
import os
from pathlib import Path

from cloud.gcp_utils import PktBigQueryWarehouse, PktGcpCredentials
from databases.snowflake_utils import PktSnowflakeConnector
from pendulum import now as pd_now
from prefect import flow, task
from prefect.runtime.flow_run import get_scheduled_start_time
from prefect_gcp.bigquery import bigquery_query
from prefect_snowflake.database import snowflake_query

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
MOZ_DATA_BQ_PROJECT = os.getenv("MOZ_DATA_BQ_PROJECT", "moz-fx-data-shared-prod")
POCKET_PREFECT_BQ_PROJECT = os.getenv(
    "POCKET_PREFECT_BQ_PROJECT", "pocket-prefect-nonprod"
)  # noqa: E501
POCKET_PREFECT_GCS_BUCKET = os.getenv(
    "POCKET_PREFECT_GCS_BUCKET", "pocket-prefect-stage-dev"
)  # noqa: E501

# EXPORT_BQ_TEMPLATE = """EXPORT DATA OPTIONS(
#   uri="gs://{pocket_prefect_gcs_bucket}/bq_to_snowflake/{snowflake_table_name}/{partition_ts}/*",
#   format='json') AS
#   {bq_query}
# """
LAST_TIMESTAMP_TEMPLATE = """select coalesce(max(airflow_execution_date), '2020-11-16 06:00:00.000') as airflow_execution_date
from prefect.mozilla.{snowflake_table_name}"""  # noqa: E501

EXPORT_BQ_TEMPLATE = """EXPORT DATA OPTIONS(
    uri="gs://{pocket_prefect_gcs_bucket}/bq_to_snowflake/{snowflake_table_name}/{partition_ts}/*",
    format='PARQUET',
    compression='SNAPPY',
    overwrite=true) AS
    with flattened as (
    SELECT s.*, flattened_tiles.id as tile_id, IFNULL(flattened_tiles.pos, alt_pos) as tile_array_position
    FROM `moz-fx-data-shared-prod.activity_stream_live.impression_stats_v1` as s
    CROSS JOIN UNNEST(s.tiles) AS flattened_tiles
    WITH OFFSET AS alt_pos
    where submission_timestamp > current_timestamp() - interval 10 minute
    AND (source IS NULL or source != 'https://newtab.firefoxchina.cn/newtab/as/activity-stream.html') --exclude custom New Tab page for FX China
    AND (page IS NULL or page != 'https://newtab.firefoxchina.cn/newtab/as/activity-stream.html') --exclude custom New Tab page for FX China
    and loaded is null --don't include loaded ping
    and array_length(tiles) >= 1 --making sure data is valid/non-empty
    )
    select u.*,
    t.type as e_tile_type
    from flattened u
    left join `moz-fx-data-shared-prod.pocket.spoc_tile_ids` as t
    on u.tile_id = t.tile_id
    qualify row_number() over (partition by document_id order by submission_timestamp desc) = 1;"""  # noqa: E501


@flow()
async def bq_to_snowflake(subflow_set_def: dict):
    last_ts = await snowflake_query(
        snowflake_connector=PktSnowflakeConnector(),
        query=LAST_TIMESTAMP_TEMPLATE.format(
            snowflake_table_name=subflow_set_def["seq_id"]
        ),
    )
    await bigquery_query(
        gcp_credentials=PktGcpCredentials(),
        query=EXPORT_BQ_TEMPLATE.format(
            pocket_prefect_gcs_bucket=POCKET_PREFECT_GCS_BUCKET,
            snowflake_table_name=subflow_set_def["seq_id"],
            partition_ts=int(get_scheduled_start_time().timestamp()),
            bq_query=EXPORT_BQ_TEMPLATE
        ),
    )
    # await snowflake_query(
    #     snowflake_connector=PktSnowflakeConnector(),
    #     query="select 1;",
    #     wait_for=[stg],  # type: ignore
    # )


async def subflow_factory(group_id: str):
    with open(os.path.join(SCRIPT_PATH, "config/bq_to_snowflake.json")) as f:
        groups = json.load(f)
    group = groups[group_id]
    coros = [bq_to_snowflake(s) for s in group["subflow_set_defs"]]
    await asyncio.gather(*coros)
