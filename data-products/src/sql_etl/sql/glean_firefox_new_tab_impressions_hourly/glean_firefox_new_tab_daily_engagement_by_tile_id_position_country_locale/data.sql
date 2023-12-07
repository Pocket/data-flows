{% set sql_engine = "bigquery" %}
{% import 'helpers.j2' as helpers with context %}

--replicates logic of current Prefect query using Glean data
--https://github.com/Pocket/data-flows/blob/main-v2/data-products/src/sql_etl/sql/firefox_new_tab_impressions_hourly/firefox_new_tab_daily_engagement_by_tile_id_position_country_locale/data.sql

WITH
  deduplicated_pings AS (
    SELECT
      *   
    FROM `moz-fx-data-shared-prod.firefox_desktop_live.newtab_v1`
    {{helpers.legacy_rolling_24_hours_filter()}}
    QUALIFY row_number() over (PARTITION BY DATE(submission_timestamp),
    document_id
    ORDER BY
    submission_timestamp desc) = 1
),
flattened_pocket_events as (
    SELECT
        submission_timestamp,
        e.name as event_name,
        mozfun.map.get_key(e.extra, 'recommendation_id') as recommendation_id,
        mozfun.map.get_key(e.extra, 'tile_id') as tile_id,
        mozfun.map.get_key(e.extra, 'position') as position,
        metrics.string.newtab_locale as locale,
        normalized_country_code as country
    FROM deduplicated_pings, UNNEST(events) as e
    --filter to Pocket events
    WHERE e.category = 'pocket'
        and e.name in ('impression', 'click', 'save', 'dismiss')
        --keep only data with a non-null recommendation ID or tile ID
        and (mozfun.map.get_key(e.extra, 'recommendation_id') is not null or mozfun.map.get_key(e.extra, 'tile_id') is not null)
        --include only data from Firefox 121+
        and SAFE_CAST(SPLIT(client_info.app_display_version, '.')[0] AS int64) >= 121
)

--let's skip the usual click deduplication step, Jeff Silverman's analysis showed that Glean is resistant to
--the duplicate clicks issue we saw with the legacy PingCentre events:
--https://docs.google.com/document/d/1aL3bjJ6PQLHH455zCMReDpYnyqPTNwTvTW2QujwP-nw/edit#heading=h.bz9b6bjjzei

SELECT
    DATE(submission_timestamp) AS happened_at,
    recommendation_id,
    tile_id,
    position,
    'CARDGRID' as source,
    locale,
    country,
    
    SUM(CASE WHEN event_name = 'impression' then 1 else 0 end) as impression_count,
    SUM(CASE WHEN event_name = 'click' then 1 else 0 end) as click_count,
    SUM(CASE WHEN event_name = 'save' then 1 else 0 end) as save_count,
    SUM(CASE WHEN event_name = 'dismiss' then 1 else 0 end) as dismiss_count
    
FROM flattened_pocket_events
GROUP BY 1, 2, 3, 4, 5, 6, 7