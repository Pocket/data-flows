import os
from typing import List, Union

import pandas as pd
from prefect import task, flow
from prefect_snowflake import SnowflakeConnector, SnowflakeCredentials
from prefect_snowflake.database import snowflake_query

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
        ORDER BY TRAILING_1_DAY_IMPRESSIONS DESC
        LIMIT 16384 -- Limit to Snowflake's max list size. There are a lot of old items that still get some impressions.
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


@task()
def df_is_empty(df: pd.DataFrame) -> bool:
    return df.empty


@flow()
def fx_newtab_aggregate_engagement():
    with open(os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')) as fp:
        key = fp.read()

    snowflake_credentials = SnowflakeCredentials(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        private_key=key,
        password=os.getenv('SNOWFLAKE_PASSPHRASE'),
    )
    snowflake_connector = SnowflakeConnector(
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        credentials=snowflake_credentials,
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
    )

    df_telemetry = pd.DataFrame.from_dict({'TILE_ID': ['1234']})

    df_corpus_item_keys = snowflake_query(
        snowflake_connector=snowflake_connector,
        query=EXPORT_CORPUS_ITEM_KEYS_SQL,
        params={'tile_ids': df_column_to_list(df_telemetry, column_name='TILE_ID')},
    )



# with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=15)) as flow:
#     # Get telemetry from BigQuery
#     df_telemetry = BigQueryTask()(
#         query=EXPORT_FIREFOX_TELEMETRY_SQL,
#         to_dataframe=True,
#     )
#
#     # For the tileIds from BigQuery, get the metadata required by corpus-engagement-v1 that's stored in Snowflake.
#     df_corpus_item_keys = PocketSnowflakeQuery()(
#         query=EXPORT_CORPUS_ITEM_KEYS_SQL,
#         data={'tile_ids': df_column_to_list(df_telemetry, column_name='TILE_ID')},
#     )
#
#     # If none of the tileIds exist in Snowflake, then there's nothing to do. This should only happen while the new API
#     # is under development, and there are days without any NewTab impressions on content served from Recommendation API.
#     with case(df_is_empty(df_corpus_item_keys), False):
#         # Combine the BigQuery and Snowflake results on TILE_ID.
#         df_keyed_telemetry = pd_merge(df_telemetry, df_corpus_item_keys)
#         # Drop TILE_ID now we no longer need it, to match the dataframe columns with the feature group.
#         df_keyed_telemetry = df_drop(df_keyed_telemetry, columns=['TILE_ID'])
#
#         dataframe_to_feature_group(
#             dataframe=df_keyed_telemetry,
#             feature_group_name=f"{config.ENVIRONMENT}-corpus-engagement-v1"
#         )


if __name__ == "__main__":
    fx_newtab_aggregate_engagement()
