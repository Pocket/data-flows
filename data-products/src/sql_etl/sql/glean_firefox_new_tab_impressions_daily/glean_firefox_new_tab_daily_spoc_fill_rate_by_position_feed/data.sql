{% set sql_engine = "bigquery" %}
{% import 'helpers.j2' as helpers with context %}
{% if for_new_offset %}
    select current_timestamp()
{% else %}

--replicates logic of current Prefect query using Glean data
--https://github.com/Pocket/data-flows/blob/main-v2/data-products/src/sql_etl/sql/firefox_new_tab_impressions_daily/firefox_new_tab_daily_spoc_fill_rate_by_position_feed/data.sql
WITH

  deduplicated_pings AS (
    SELECT
      *   
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_live.newtab_v1`
    WHERE submission_timestamp >= {{ helpers.parse_iso8601(batch_start) }}
    AND submission_timestamp < {{ helpers.parse_iso8601(batch_end) }}
    QUALIFY row_number() over (PARTITION BY DATE(submission_timestamp),
    document_id
    ORDER BY
    submission_timestamp desc) = 1
),
  flattened_impression_data as (
    SELECT
        DATE(submission_timestamp) as happened_at,
        CASE
            WHEN ( normalized_country_code IN ('US', 'CA') AND metrics.string.newtab_locale IN ('en-CA', 'en-GB', 'en-US') ) THEN 'NEW_TAB_EN_US'
            WHEN ( normalized_country_code IN ('GB',
              'IE')
            AND metrics.string.newtab_locale IN ('en-CA',
              'en-GB',
              'en-US') ) THEN 'NEW_TAB_EN_GB'
            WHEN ( normalized_country_code IN ('IN') AND metrics.string.newtab_locale IN ('en-CA', 'en-GB', 'en-US') ) THEN 'NEW_TAB_EN_INTL'
            WHEN ( normalized_country_code IN ('DE',
              'CH',
              'AT',
              'BE')
            AND metrics.string.newtab_locale IN ('de',
              'de-AT',
              'de-CH') ) THEN 'NEW_TAB_DE_DE'
            WHEN (normalized_country_code IN ('IT') AND metrics.string.newtab_locale IN ('it')) THEN 'NEW_TAB_IT_IT'
            WHEN (normalized_country_code IN ('FR')
            AND metrics.string.newtab_locale IN ('fr')) THEN 'NEW_TAB_FR_FR'
            WHEN (normalized_country_code IN ('ES') AND metrics.string.newtab_locale IN ('es-ES')) THEN 'NEW_TAB_ES_ES'
        END
          AS feed_name,
        SAFE_CAST(mozfun.map.get_key(e.extra, 'position') AS int64) as position,
        SAFE_CAST(mozfun.map.get_key(e.extra, 'is_sponsored') AS boolean) as is_sponsored,
        COUNT(1) as impressions
    FROM deduplicated_pings, UNNEST(events) as e
    --filter to Pocket events
    WHERE e.category = 'pocket'
        --limit to impressions
        and e.name = 'impression'
        --keep only data with a non-null recommendation ID or tile ID
        and (mozfun.map.get_key(e.extra, 'recommendation_id') is not null or mozfun.map.get_key(e.extra, 'tile_id') is not null)
        --include only release channel data
        and normalized_channel = 'release'
        --include only data from Firefox 121+
        and SAFE_CAST(SPLIT(client_info.app_display_version, '.')[0] AS int64) >= 121
        --include only users with both Pocket and SPOCs enabled
        and metrics.boolean.pocket_enabled = True
        and metrics.boolean.pocket_sponsored_stories_enabled = True
        --limit to locale & country combinations with Pocket enabled
        AND ( ( normalized_country_code IN ('US',
              'CA',
              'GB',
              'IE',
              'IN')
            AND metrics.string.newtab_locale IN ('en-CA',
              'en-GB',
              'en-US') )
            OR ( normalized_country_code IN ('DE',
              'CH',
              'AT',
              'BE')
            AND metrics.string.newtab_locale IN ('de',
              'de-AT',
              'de-CH') )
            OR (normalized_country_code IN ('IT')
            AND metrics.string.newtab_locale IN ('it'))
            OR (normalized_country_code IN ('FR')
            AND metrics.string.newtab_locale IN ('fr'))
            OR (normalized_country_code IN ('ES')
            AND metrics.string.newtab_locale IN ('es-ES')) )
    group by 1, 2, 3, 4
),

new_tab_impressions AS ( --use impressions of first position as a proxy for # of new tab opens
  SELECT
    happened_at,
    feed_name,
    SUM(impressions) AS new_tab_impressions
  FROM
    flattened_impression_data
  WHERE
    position = 0
  GROUP BY
    1, 2),
    
impression_counts AS (
  SELECT
    a.happened_at,
    a.feed_name,
    a.position,
    MIN(n.new_tab_impressions) AS new_tab_impression_count,
    SUM(a.impressions) AS position_total_impression_count,
    SUM(CASE
        WHEN a.is_sponsored = True THEN a.impressions
    END
      ) AS position_spoc_impression_count,
  FROM
    flattened_impression_data AS a
  JOIN
    new_tab_impressions AS n
  ON
    n.happened_at = a.happened_at
        and n.feed_name = a.feed_name
  WHERE
    a.position IN (2,
      4,
      11,
      20)
    OR a.position IN (1,
      5,
      7,
      11,
      18,
      20)
  GROUP BY
    1,
    2,
    3
  ORDER BY
    1,
    2,
    3)

SELECT
  happened_at,
  feed_name,
  position,
  new_tab_impression_count,
  position_total_impression_count,
  IFNULL(position_spoc_impression_count, 0) AS position_spoc_impression_count,
FROM
  impression_counts
ORDER BY
  1,
  2,
  3
{% endif %}