import os
from asyncio import gather, run
from typing import NamedTuple

import pandas as pd
from common.cloud.gcp_utils import MozGcpCredentialsV2 as MozGcp
from common.databases.snowflake_utils import MozSnowflakeConnector
from common.deployment.worker import FlowDeployment, FlowSpec
from common.settings import CommonSettings
from prefect import flow, get_run_logger
from prefect_gcp.bigquery import bigquery_query
from prefect_snowflake.database import snowflake_query
from shared.feature_store import FeatureGroupSettings, dataframe_to_feature_group
from snowflake.connector import DictCursor

CS = CommonSettings()  # type: ignore


def optional_suffix():
    value = f"_{CS.deployment_type}"
    if value == "_main":
        value = ""
    return value


NEW_TAB_REC_DATASET = os.getenv(
    "NEW_TAB_REC_DATASET", f"new_tab_recommendations{optional_suffix()}"
)
NEW_TAB_REC_GCP_PROJECT_ENV_MAP = {"dev": "nonprod", "production": "prod"}

NEW_TAB_REC_GCP_PROJECT_ENV = NEW_TAB_REC_GCP_PROJECT_ENV_MAP[CS.dev_or_production]

EXPORT_ACTIVITY_STREAM_TELEMETRY_SQL = """
        WITH impressions_data AS (
            SELECT s.*
            FROM `moz-fx-data-shared-prod.activity_stream_live.impression_stats_v1` AS s
            WHERE submission_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
            -- addon_version (Fx build id) must be less than the Glean build below to avoid double-counting impressions
            AND addon_version < '20231116134553'
            AND (source = 'TOP_STORIES' OR source = 'CARDGRID')
            AND loaded IS NULL
            AND normalized_country_code IS NOT NULL
            AND array_length(tiles) >= 1
        )
        SELECT
            CAST(flattened_tiles.id AS INT64) AS TILE_ID,
            FORMAT_DATETIME("%Y-%m-%dT%H:%M:%SZ", MAX(submission_timestamp)) as UPDATED_AT,  -- Feature Store requires ISO 8601 time format
            COUNT(*) AS TRAILING_1_DAY_IMPRESSIONS,
            SUM(CASE WHEN click IS NOT NULL THEN 1 ELSE 0 END) AS TRAILING_1_DAY_OPENS
        FROM impressions_data
        CROSS JOIN UNNEST(impressions_data.tiles) AS flattened_tiles
        GROUP BY TILE_ID
        ORDER BY TRAILING_1_DAY_IMPRESSIONS DESC
        LIMIT 16384 -- Limit to Snowflake's max list size. There are a lot of old items that still get some impressions.
    """  # noqa: E501

EXPORT_GLEAN_TELEMETRY_SQL = f"""
DECLARE max_ts timestamp;

-- create dataset
create schema if not exists `moz-fx-mozsocial-dw-{NEW_TAB_REC_GCP_PROJECT_ENV}.{NEW_TAB_REC_DATASET}`;

-- table if not exists
create table if not exists `moz-fx-mozsocial-dw-{NEW_TAB_REC_GCP_PROJECT_ENV}.{NEW_TAB_REC_DATASET}.pocket_user_events` as (
      SELECT
        document_id,
        submission_timestamp,
        event.name AS event_name,
        extra.value AS recommendation_id,
        current_timestamp() AS _loaded_at
      FROM `moz-fx-data-shared-prod.firefox_desktop.newtab_live` AS e
      CROSS JOIN UNNEST(e.events) AS event
      CROSS JOIN UNNEST(event.extra) AS extra ON extra.key = 'recommendation_id'
      WHERE
        submission_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
        AND 1 = 2
);
-- get max submission timestamp from table
SET max_ts = (select coalesce(max(submission_timestamp), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)) as max_ts
from `moz-fx-mozsocial-dw-{NEW_TAB_REC_GCP_PROJECT_ENV}.{NEW_TAB_REC_DATASET}.pocket_user_events`);

-- insert new records
insert into `moz-fx-mozsocial-dw-{NEW_TAB_REC_GCP_PROJECT_ENV}.{NEW_TAB_REC_DATASET}.pocket_user_events`
      SELECT
        document_id,
        submission_timestamp,
        event.name AS event_name,
        extra.value AS recommendation_id,
        current_timestamp() AS _loaded_at
      FROM `moz-fx-data-shared-prod.firefox_desktop.newtab_live` AS e
      CROSS JOIN UNNEST(e.events) AS event
      CROSS JOIN UNNEST(event.extra) AS extra ON extra.key = 'recommendation_id'
      WHERE
        submission_timestamp > max_ts
        AND app_build >= '20231116134553' -- Fx 120 was the first build to emit recommendation_id
        AND event.category = 'pocket'
        AND event.name in ('click', 'impression');
-- get aggregations
  SELECT
      recommendation_id as CORPUS_RECOMMENDATION_ID,
      FORMAT_DATETIME("%Y-%m-%dT%H:%M:%SZ", MAX(submission_timestamp)) as UPDATED_AT,  -- Feature Store requires ISO 8601 time format
      APPROX_COUNT_DISTINCT(IF(event_name = 'impression', document_id, NULL)) AS TRAILING_1_DAY_IMPRESSIONS,
      APPROX_COUNT_DISTINCT(IF(event_name = 'click', document_id, NULL)) AS TRAILING_1_DAY_OPENS
  FROM `moz-fx-mozsocial-dw-{NEW_TAB_REC_GCP_PROJECT_ENV}.{NEW_TAB_REC_DATASET}.pocket_user_events`
  where submission_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  GROUP BY recommendation_id
  ORDER BY TRAILING_1_DAY_IMPRESSIONS DESC
  LIMIT 16384; -- Limit to Snowflake's max list size. Can be removed once we don't join across BQ/Snowflake anymore."""  # noqa: E501


EXPORT_CORPUS_ITEM_KEYS_SQL = """
    SELECT DISTINCT 
        {JOIN_COLUMN_NAME},
        concat_ws(
            -- corpus-engagement-v1 is keyed on the following three fields, separated by slashes.
            '/', recommendation_surface_id, corpus_slate_configuration_id, corpus_item_id
        ) as KEY,
        RECOMMENDATION_SURFACE_ID,
        CORPUS_SLATE_CONFIGURATION_ID,
        CORPUS_ITEM_ID
    FROM ANALYTICS.DBT_STAGING.STG_CORPUS_SLATE_RECOMMENDATIONS
    WHERE {JOIN_COLUMN_NAME} in (%(ID_LIST)s)
    -- Speed up query using clustered column. 28 day recommendation window was chosen arbitrarily to cover 1 day 
    -- trailing telemetry. It executes in 35 seconds on XS.
    AND recommended_at > DATEADD(DAY, -28, CURRENT_TIMESTAMP())
"""  # noqa: E501


class TelemetrySource(NamedTuple):
    export_telemetry_sql: str
    join_column_name: str


"""
Glean events have a 'recommendation id' (a.k.a. 'corpus recommendation id'), 
which is distinct for each server-side
event when content is recommended. Activity Stream uses the older 'tile id', 
which is distinct for each time content is
scheduled, and cannot be traced back to a single server-side recommendation event.

The ability to trace back to server-side events improves observability 
and enables experiments with rankers.
"""
telemetry_sources: list[TelemetrySource] = [
    TelemetrySource(
        EXPORT_GLEAN_TELEMETRY_SQL, "CORPUS_RECOMMENDATION_ID"
    ),  # Glean is joined on recommendation UUID.
    TelemetrySource(
        EXPORT_ACTIVITY_STREAM_TELEMETRY_SQL, "TILE_ID"
    ),  # Activity stream is joined on integer tile id.
]


async def export_telemetry_by_corpus_item_id(
    export_telemetry_sql: str, join_column_name: str
):
    """
    Exports `export_telemetry_sql` from BigQuery,
    joined with corpus metadata from Snowflake on `join_column_name`.
    :param export_telemetry_sql: BigQuery SQL to export open and impression counts.
    Must export `join_column_name`.
    :param join_column_name: Column in STG_CORPUS_SLATE_RECOMMENDATIONS to join on.
    :return: Telemetry aggregated by CORPUS_ITEM_ID
    """
    # join_column_name is a string literal,
    # so there's no risk of SQL injection by inserting it in the query below.
    export_corpus_item_keys_sql = EXPORT_CORPUS_ITEM_KEYS_SQL.format(
        JOIN_COLUMN_NAME=join_column_name
    )
    df_telemetry = await bigquery_query(
        gcp_credentials=MozGcp(),
        query=export_telemetry_sql,
        to_dataframe=True,
    )
    corpus_item_keys_records = await snowflake_query(
        snowflake_connector=MozSnowflakeConnector(),
        query=export_corpus_item_keys_sql,
        cursor_type=DictCursor,  # type: ignore
        params={"ID_LIST": df_telemetry[join_column_name].tolist()},
    )

    df_corpus_item_keys = pd.DataFrame(corpus_item_keys_records)
    # Combine the BigQuery and Snowflake results on TILE_ID.
    df_telemetry = pd.merge(df_telemetry, df_corpus_item_keys)

    # Drop TILE_ID or CORPUS_RECOMMENDATION_ID after merging,
    # to match the dataframe columns with the feature group.
    return df_telemetry.drop(columns=[join_column_name])


@flow(name="new-tab-recommendations.aggregate-engagement")
async def aggregate_engagement():
    """
    Ingests NewTab telemetry joined with Corpus metadata into a Sagemaker Feature Group.
    """
    logger = get_run_logger()
    # Export telemetry from Glean and Activity Stream aggregated by CORPUS_ITEM_ID.
    all_dataframes = await gather(
        *[
            export_telemetry_by_corpus_item_id(export_sql, join_column)
            for export_sql, join_column in telemetry_sources
        ]
    )
    logger.info(f"Dataframe counts are: {[len(x) for x in all_dataframes]}...")
    # Sum the opens and impressions from Glean and Acitivy Stream.
    df_combined = (
        pd.concat(all_dataframes)
        .groupby(
            [
                "UPDATED_AT",
                "KEY",
                "RECOMMENDATION_SURFACE_ID",
                "CORPUS_SLATE_CONFIGURATION_ID",
                "CORPUS_ITEM_ID",
            ]
        )
        .sum()
        .reset_index()
    )

    # Feature Store requires all columns to be present.
    # We only need 1 day trailing data, so add the others as 0.
    df_combined["TRAILING_7_DAY_IMPRESSIONS"] = 0
    df_combined["TRAILING_7_DAY_OPENS"] = 0
    df_combined["TRAILING_14_DAY_IMPRESSIONS"] = 0
    df_combined["TRAILING_14_DAY_OPENS"] = 0
    df_combined["TRAILING_21_DAY_IMPRESSIONS"] = 0
    df_combined["TRAILING_21_DAY_OPENS"] = 0
    df_combined["TRAILING_28_DAY_IMPRESSIONS"] = 0
    df_combined["TRAILING_28_DAY_OPENS"] = 0

    await dataframe_to_feature_group(
        dataframe=df_combined,
        feature_group_name=FeatureGroupSettings().corpus_engagement_feature_group_name,
    )


FLOW_SPEC = FlowSpec(
    flow=aggregate_engagement,
    docker_env="base",
    deployments=[
        FlowDeployment(
            name="deployment",
            cron="*/15 * * * *",
            job_variables={
                "cpu": 4096,
                "memory": 8192,
                "env": {"PREFECT_API_ENABLE_HTTP2": str(False)},
            },
        ),
    ],
)


if __name__ == "__main__":
    run(aggregate_engagement())
