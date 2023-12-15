from asyncio import run

import pandas as pd
from common.cloud.gcp_utils import MozGcpCredentials
from common.databases.snowflake_utils import MozSnowflakeConnector
from common.deployment import FlowSpec, FlowEnvar, FlowDeployment
from common.settings import CommonSettings
from prefect import flow
from prefect.server.schemas.schedules import CronSchedule
from prefect_gcp.bigquery import bigquery_query
from prefect_snowflake.database import snowflake_query
from snowflake.connector import DictCursor

from shared.feature_store import dataframe_to_feature_group, FeatureGroupSettings

CS = CommonSettings()  # type: ignore

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
            FORMAT_DATETIME("%Y-%m-%dT%H:%M:%SZ", MAX(submission_timestamp)) as UPDATED_AT,  -- Feature Store requires ISO 8601 time format
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

EXPORT_GLEAN_TELEMETRY_SQL = """
    WITH events AS (
      SELECT
        document_id,
        submission_timestamp,
        event.name AS event_name,
        extra.value AS recommendation_id
      FROM `moz-fx-data-shared-prod.firefox_desktop_live.newtab_v1` AS e
      CROSS JOIN UNNEST(e.events) AS event
      CROSS JOIN UNNEST(event.extra) AS extra ON extra.key = 'recommendation_id'
      WHERE
        submission_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
        AND client_info.app_build >= '20231116134553' -- Fx 120 was the first build to emit recommendation_id
        AND event.category = 'pocket'
        AND event.name in ('click', 'impression')
    )
    SELECT
        recommendation_id as CORPUS_RECOMMENDATION_ID,
        FORMAT_DATETIME("%Y-%m-%dT%H:%M:%SZ", MAX(submission_timestamp)) as UPDATED_AT,  -- Feature Store requires ISO 8601 time format
        APPROX_COUNT_DISTINCT(IF(event_name = 'impression', document_id, NULL)) AS TRAILING_1_DAY_IMPRESSIONS,
        APPROX_COUNT_DISTINCT(IF(event_name = 'click', document_id, NULL)) AS TRAILING_1_DAY_OPENS
    FROM events
    GROUP BY recommendation_id
    ORDER BY TRAILING_1_DAY_IMPRESSIONS DESC
    LIMIT 16384 -- Limit to Snowflake's max list size. Can be removed once we don't join across BQ/Snowflake anymore.
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


@flow()
async def aggregate_engagement():
    df_telemetry = await bigquery_query(
        gcp_credentials=MozGcpCredentials(),
        query=EXPORT_FIREFOX_TELEMETRY_SQL,
        to_dataframe=True,
    )

    corpus_item_keys_records = await snowflake_query(
        snowflake_connector=MozSnowflakeConnector(),
        query=EXPORT_CORPUS_ITEM_KEYS_SQL,
        cursor_type=DictCursor,
        params={"tile_ids": df_telemetry["TILE_ID"].tolist()},
    )

    if corpus_item_keys_records:
        df_corpus_item_keys = pd.DataFrame(corpus_item_keys_records)
        # Combine the BigQuery and Snowflake results on TILE_ID.
        df_keyed_telemetry = pd.merge(df_telemetry, df_corpus_item_keys)
        # Drop TILE_ID now we no longer need it, to match the dataframe columns with the feature group.
        df_keyed_telemetry = df_keyed_telemetry.drop(columns=["TILE_ID"])

        await dataframe_to_feature_group(
            dataframe=df_keyed_telemetry,
            feature_group_name=FeatureGroupSettings().corpus_engagement_feature_group_name,
        )


FLOW_SPEC = FlowSpec(
    flow=aggregate_engagement,
    docker_env="base",
    secrets=[
        FlowEnvar(
            envar_name="DF_CONFIG_SNOWFLAKE_CREDENTIALS",
            envar_value=f"data-flows/{CS.deployment_type}/snowflake-credentials",
        ),
        FlowEnvar(
            envar_name="DF_CONFIG_GCP_CREDENTIALS",
            envar_value=f"data-flows/{CS.deployment_type}/gcp-credentials",
        ),
        FlowEnvar(
            envar_name="DF_CONFIG_SNOWFLAKE_GCP_STAGES",
            envar_value=f"data-flows/{CS.deployment_type}/snowflake-gcp-stage-data",
        ),
    ],
    envars=[
        FlowEnvar(
            envar_name="PREFECT_API_ENABLE_HTTP2",
            envar_value="False",
        )
    ],
    deployments=[
        FlowDeployment(
            deployment_name="deployment",
            schedule=CronSchedule(cron="*/15 * * * *"),
            cpu="4096",
            memory="8192",
        ),
    ],
)


if __name__ == "__main__":
    run(aggregate_engagement())
