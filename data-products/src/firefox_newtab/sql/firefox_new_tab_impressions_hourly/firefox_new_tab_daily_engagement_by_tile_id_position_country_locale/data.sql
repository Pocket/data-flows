{% set sql_engine = "bigquery" %}
{% import 'helpers.j2' as helpers with context %}
WITH
  deduplicated AS (
    SELECT
      *   
    FROM
      `moz-fx-data-shared-prod.activity_stream_live.impression_stats_v1`  
    {{helpers.legacy_rolling_24_hours_filter()}}
    QUALIFY row_number() over (PARTITION BY DATE(submission_timestamp),
    document_id
    ORDER BY
    submission_timestamp desc) = 1
),
impression_data AS (
  SELECT
    s.*
  FROM
    deduplicated AS s
  WHERE
    loaded IS NULL --don't include loaded ping
    AND SAFE_CAST(SPLIT(version, '.')[0] AS int64) <= 120 --include only data from Firefox < 121
    AND ARRAY_LENGTH(tiles) >= 1 --make sure data is valid/non-empty
    AND (page IS NULL
      OR page != 'https://newtab.firefoxchina.cn/newtab/as/activity-stream.html') --exclude custom New Tab page for FX China
    ),
  flattened_impression_data AS ( --need this step to filter out >2 clicks from a given client on the same tile within 1 second
  SELECT
    UNIX_SECONDS(submission_timestamp) AS submission_timestamp,
    --truncate timestamp to seconds
    impression_id AS client_id,
    --client_id renamed to impression_id in GCP
    flattened_tiles.id AS tile_id,
    IFNULL(flattened_tiles.pos, alt_pos) AS position,
    --the 3x1 layout has a bug where we need to use the position of each element in the tiles array instead of the actual pos field
    source,
    locale,
    normalized_country_code AS country,
    SUM(CASE
        WHEN click IS NULL AND block IS NULL AND pocket IS NULL THEN 1
      ELSE
      0
    END
      ) AS impressions,
    SUM(CASE
        WHEN click IS NOT NULL THEN 1
      ELSE
      0
    END
      ) AS clicks,
    SUM(CASE
        WHEN pocket IS NOT NULL THEN 1
      ELSE
      0
    END
      ) AS pocketed,
    SUM(CASE
        WHEN block IS NOT NULL THEN 1
      ELSE
      0
    END
      ) AS blocked
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
    4,
    5,
    6,
    7 )
SELECT
  DATE(TIMESTAMP_SECONDS(submission_timestamp)) AS happened_at,
  tile_id,
  position,
  source,
  locale,
  country,
  SUM(impressions) AS impression_count,
  SUM(clicks) AS click_count,
  SUM(pocketed) AS save_count,
  SUM(blocked) AS dismiss_count,
FROM
  flattened_impression_data
WHERE
  clicks < 3
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6
ORDER BY
  1,
  2,
  3,
  4,
  5,
  6;