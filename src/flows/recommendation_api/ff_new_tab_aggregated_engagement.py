from typing import List, Union

import pandas as pd
from prefect import Flow, task
from prefect.tasks.gcp.bigquery import BigQueryTask

from api_clients.pocket_snowflake_query import PocketSnowflakeQuery
from common_tasks.load_data import dataframe_to_feature_group
from utils import config
from utils.flow import get_flow_name, get_interval_schedule

FLOW_NAME = get_flow_name(__file__)

EXPORT_FIREFOX_TELEMETRY_SQL = """
        WITH impressions_data AS (
            SELECT s.*
            FROM `moz-fx-data-shared-prod.activity_stream_live.impression_stats_v1` AS s
            WHERE submission_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
            AND (source = 'TOP_STORIES' OR source = 'CARDGRID')
            AND loaded IS NULL
            AND normalized_country_code IS NOT NULL
            AND array_length(tiles) >= 1
        )
        (
        SELECT
            CAST(flattened_tiles.id AS INT64) AS TILE_ID,
            FORMAT_DATETIME("%Y-%m-%dT%H:%M:%SZ", CURRENT_DATETIME()) as UPDATED_AT,  -- Feature Store requires ISO 8601 time format
            COUNT(*) AS TRAILING_1_DAY_IMPRESSIONS,
            SUM(CASE WHEN click IS NOT NULL THEN 1 ELSE 0 END) AS TRAILING_1_DAY_OPENS,
            -- For now, we only need 1 day trailing data, so leave the other ones at 0.  
            0 AS TRAILING_7_DAY_IMPRESSIONS,
            0 AS TRAILING_7_DAY_OPENS,
            0 AS TRAILING_14_DAY_IMPRESSIONS,
            0 AS TRAILING_14_DAY_OPENS,
            0 AS TRAILING_21_DAY_IMPRESSIONS,
            0 AS TRAILING_21_DAY_OPENS,
            0 AS TRAILING_28_DAY_IMPRESSIONS,
            0 AS TRAILING_28_DAY_OPENS
        FROM impressions_data
        CROSS JOIN UNNEST(impressions_data.tiles) AS flattened_tiles
        GROUP BY TILE_ID
        HAVING TRAILING_1_DAY_IMPRESSIONS > 1000 -- removes content that isn't actively served but still gets impressions.
        )
        
        -- Dummy data from example Snowplow event 8c608112-b1c4-4354-b66b-9f00324e064f
        -- Remove once we get 
        union all select 1018311686533393, '2023-04-04T10:02:00Z', 1000, 93, 0, 0, 0, 0, 0, 0, 0, 0
        union all select 2336135188504018, '2023-04-04T10:02:00Z', 1000, 18, 0, 0, 0, 0, 0, 0, 0, 0
        union all select 1196919788541269, '2023-04-04T10:02:00Z', 1000, 69, 0, 0, 0, 0, 0, 0, 0, 0
        union all select 3696711462613678, '2023-04-04T10:02:00Z', 1000, 78, 0, 0, 0, 0, 0, 0, 0, 0
        union all select 3899469774231035, '2023-04-04T10:02:00Z', 1000, 35, 0, 0, 0, 0, 0, 0, 0, 0
        union all select 2137436018802803, '2023-04-04T10:02:00Z', 1000, 3, 0, 0, 0, 0, 0, 0, 0, 0
        union all select 2516923037524350, '2023-04-04T10:02:00Z', 1000, 50, 0, 0, 0, 0, 0, 0, 0, 0
        union all select 2213560505990250, '2023-04-04T10:02:00Z', 1000, 50, 0, 0, 0, 0, 0, 0, 0, 0
        union all select 2406475520295944, '2023-04-04T10:02:00Z', 1000, 44, 0, 0, 0, 0, 0, 0, 0, 0
        union all select 3327856852376222, '2023-04-04T10:02:00Z', 1000, 22, 0, 0, 0, 0, 0, 0, 0, 0
        union all select 903421925709054, '2023-04-04T10:02:00Z', 1000, 54, 0, 0, 0, 0, 0, 0, 0, 0
        union all select 4127678838378836, '2023-04-04T10:02:00Z', 1000, 36, 0, 0, 0, 0, 0, 0, 0, 0
        union all select 768277328123901, '2023-04-04T10:02:00Z', 1000, 1, 0, 0, 0, 0, 0, 0, 0, 0
        union all select 2141616218975497, '2023-04-04T10:02:00Z', 1000, 97, 0, 0, 0, 0, 0, 0, 0, 0
        union all select 2872260490206165, '2023-04-04T10:02:00Z', 1000, 65, 0, 0, 0, 0, 0, 0, 0, 0
        union all select 2594624165134442, '2023-04-04T10:02:00Z', 1000, 42, 0, 0, 0, 0, 0, 0, 0, 0
        union all select 3796701044018254, '2023-04-04T10:02:00Z', 1000, 54, 0, 0, 0, 0, 0, 0, 0, 0
    """


EXPORT_CORPUS_ITEM_KEYS_SQL = """
    SELECT DISTINCT 
        TILE_ID,
        concat_ws(
            -- corpus-engagement-v1 is keyed on the following three fields, separated by slashes.
            '/', recommendation_surface_id, corpus_slate_configuration_id, corpus_item_id
        ) as KEY,
        RECOMMENDATION_SURFACE_ID,
        CORPUS_SLATE_CONFIGURATION_ID,
        CORPUS_ITEM_ID
    FROM ANALYTICS.DBT_STAGING.STG_CORPUS_SLATE_RECOMMENDATIONS
    WHERE TILE_ID in (%(tile_ids)s)
"""


@task()
def df_column_to_list(df: pd.DataFrame, column_name: str):
    return df[column_name].tolist()


@task()
def pd_merge(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    return pd.merge(left, right)


@task()
def df_drop(df: pd.DataFrame, columns: Union[str, List[str]]) -> pd.DataFrame:
    return df.drop(columns=columns)


with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=15)) as flow:
    # Get telemetry from BigQuery
    df_telemetry = BigQueryTask()(
        query=EXPORT_FIREFOX_TELEMETRY_SQL,
        to_dataframe=True,
    )

    # For the tileIds from BigQuery, get the metadata required by corpus-engagement-v1 that's stored in Snowflake.
    df_corpus_item_keys = PocketSnowflakeQuery()(
        query=EXPORT_CORPUS_ITEM_KEYS_SQL,
        data={'tile_ids': df_column_to_list(df_telemetry, column_name='TILE_ID')},
    )

    # Combine the BigQuery and Snowflake results on TILE_ID.
    df_keyed_telemetry = pd_merge(df_telemetry, df_corpus_item_keys)
    # Drop TILE_ID now we no longer need it, to match the dataframe columns with the feature group.
    df_keyed_telemetry = df_drop(df_keyed_telemetry, columns=['TILE_ID'])

    dataframe_to_feature_group(
        dataframe=df_keyed_telemetry,
        feature_group_name=f"{config.ENVIRONMENT}-corpus-engagement-v1"
    )

if __name__ == "__main__":
    flow.run()
