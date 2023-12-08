{% set sql_engine = "bigquery" %}
{% import 'helpers.j2' as helpers with context %}

--replicates logic of current Prefect query using Glean data
--https://github.com/Pocket/data-flows/blob/main-v2/data-products/src/sql_etl/sql/firefox_new_tab_impressions_hourly/firefox_new_tab_daily_engagement_by_tile_id_position_country_locale/data.sql

WITH
  deduplicated_pings AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.newtab_v1` {{helpers.legacy_rolling_24_hours_filter()}} QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE(submission_timestamp),
      document_id
    ORDER BY
      submission_timestamp DESC) = 1 ),
  flattened_pocket_events AS (
  SELECT
    submission_timestamp,
    e.name AS event_name,
    mozfun.map.get_key(e.extra,
      'recommendation_id') AS recommendation_id,
    mozfun.map.get_key(e.extra,
      'tile_id') AS tile_id,
    mozfun.map.get_key(e.extra,
      'position') AS position,
    metrics.string.newtab_locale AS locale,
    normalized_country_code AS country
  FROM
    deduplicated_pings,
    UNNEST(events) AS e
    --filter to Pocket events
  WHERE
    e.category = 'pocket'
    AND e.name IN ('impression',
      'click',
      'save',
      'dismiss')
    --keep only data with a non-null recommendation ID or tile ID
    AND (mozfun.map.get_key(e.extra,
        'recommendation_id') IS NOT NULL
      OR mozfun.map.get_key(e.extra,
        'tile_id') IS NOT NULL)
    --include only data from Firefox 121+
    AND SAFE_CAST(SPLIT(client_info.app_display_version, '.')[0] AS int64) >= 121 )
SELECT
  DATE(submission_timestamp) AS happened_at,
  recommendation_id,
  tile_id,
  position,
  'CARDGRID' AS SOURCE,
  locale,
  country,
  SUM(CASE
      WHEN event_name = 'impression' THEN 1
    ELSE
    0
  END
    ) AS impression_count,
  SUM(CASE
      WHEN event_name = 'click' THEN 1
    ELSE
    0
  END
    ) AS click_count,
  SUM(CASE
      WHEN event_name = 'save' THEN 1
    ELSE
    0
  END
    ) AS save_count,
  SUM(CASE
      WHEN event_name = 'dismiss' THEN 1
    ELSE
    0
  END
    ) AS dismiss_count
FROM
  flattened_pocket_events
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7