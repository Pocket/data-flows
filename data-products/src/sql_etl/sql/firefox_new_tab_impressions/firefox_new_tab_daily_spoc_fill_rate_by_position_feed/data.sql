{% set sql_engine = "bigquery" %}
{% if for_new_offset %}
    select current_timestamp()
{% else %}
{% macro parse_iso8601(datetime) %}
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', '{{ datetime }}')
{% endmacro %}

WITH
  deduplicated AS (
    SELECT
      *   
    FROM
      {% if for_backfill %}
      `moz-fx-data-shared-prod.activity_stream_stable.impression_stats_v1`
    {% else %}
      `moz-fx-data-shared-prod.activity_stream_live.impression_stats_v1`  
    {% endif %}
    WHERE submission_timestamp BETWEEN DATETIME_SUB({{ parse_iso8601(batch_start) }}, INTERVAL 1 DAY) AND {{ parse_iso8601(batch_start) }}
    QUALIFY row_number() over (PARTITION BY DATE(submission_timestamp),
    document_id
    ORDER BY
    submission_timestamp desc) = 1
),
  impression_data AS (
  SELECT
    s.*,
    CASE
      WHEN ( normalized_country_code IN ('US', 'CA') AND locale IN ('en-CA', 'en-GB', 'en-US') ) THEN 'NEW_TAB_EN_US'
      WHEN ( normalized_country_code IN ('GB',
        'IE')
      AND locale IN ('en-CA',
        'en-GB',
        'en-US') ) THEN 'NEW_TAB_EN_GB'
      WHEN ( normalized_country_code IN ('IN') AND locale IN ('en-CA', 'en-GB', 'en-US') ) THEN 'NEW_TAB_EN_INTL'
      WHEN ( normalized_country_code IN ('DE',
        'CH',
        'AT',
        'BE')
      AND locale IN ('de',
        'de-AT',
        'de-CH') ) THEN 'NEW_TAB_DE_DE'
      WHEN (normalized_country_code IN ('IT') AND locale IN ('it')) THEN 'NEW_TAB_IT_IT'
      WHEN (normalized_country_code IN ('FR')
      AND locale IN ('fr')) THEN 'NEW_TAB_FR_FR'
      WHEN (normalized_country_code IN ('ES') AND locale IN ('es-ES')) THEN 'NEW_TAB_ES_ES'
  END
    AS feed_name
  FROM
    deduplicated AS s
  WHERE
    loaded IS NULL --don't include loaded ping
    AND ARRAY_LENGTH(tiles) >= 1 --make sure data is valid/non-empty
    AND click IS NULL
    AND block IS NULL
    AND pocket IS NULL --impressions only
    AND normalized_country_code = 'US'
    AND locale LIKE 'en%'
    AND release_channel = 'release'
    AND user_prefs & 4 = 4
    AND user_prefs & 32 = 32
    AND ( ( normalized_country_code IN ('US',
          'CA',
          'GB',
          'IE',
          'IN')
        AND locale IN ('en-CA',
          'en-GB',
          'en-US') )
      OR ( normalized_country_code IN ('DE',
          'CH',
          'AT',
          'BE')
        AND locale IN ('de',
          'de-AT',
          'de-CH') )
      OR (normalized_country_code IN ('IT')
        AND locale IN ('it'))
      OR (normalized_country_code IN ('FR')
        AND locale IN ('fr'))
      OR (normalized_country_code IN ('ES')
        AND locale IN ('es-ES')) ) ),
  flattened_impression_data AS (
  SELECT
    UNIX_SECONDS(submission_timestamp) AS submission_timestamp,
    --truncate timestamp to seconds
    feed_name,
    flattened_tiles.id AS tile_id,
    IFNULL(flattened_tiles.pos, alt_pos) AS position,
    --the 3x1 layout has a bug where we need to use the position of each element in the tiles array instead of the actual pos field
    SUM(1) AS impressions
  FROM
    impression_data
  CROSS JOIN
    UNNEST(impression_data.tiles) AS flattened_tiles
  WITH
  OFFSET
    AS alt_pos
  GROUP BY
    1,
    2,
    3,
    4 ),
  new_tab_impressions AS ( --use impressions of first position as a proxy for # of new tab opens
  SELECT
    DATE(TIMESTAMP_SECONDS(submission_timestamp)) AS activity_date,
    SUM(impressions) AS new_tab_impressions
  FROM
    flattened_impression_data
  WHERE
    position = 0
  GROUP BY
    1 ),
  impression_counts AS (
  SELECT
    DATE(TIMESTAMP_SECONDS(a.submission_timestamp)) AS happened_at,
    feed_name,
    a.position,
    MIN(n.new_tab_impressions) AS new_tab_impression_count,
    SUM(a.impressions) AS position_total_impression_count,
    SUM(CASE
        WHEN t.type = 'spoc' THEN a.impressions
    END
      ) AS position_spoc_impression_count,
  FROM
    flattened_impression_data AS a
  LEFT JOIN
    `moz-fx-data-shared-prod.pocket.spoc_tile_ids` AS t
  ON
    a.tile_id = t.tile_id
  JOIN
    new_tab_impressions AS n
  ON
    n.activity_date = DATE(TIMESTAMP_SECONDS(a.submission_timestamp))
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
  current_timestamp AS snowflake_loaded_at
FROM
  impression_counts
ORDER BY
  1,
  2,
  3
{% endif %}