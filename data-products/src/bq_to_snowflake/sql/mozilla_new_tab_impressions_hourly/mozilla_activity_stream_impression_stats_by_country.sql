-- original path/file: dags/mozilla_stats_hourly_bq/mozilla_stats_by_source_country_export.sql

WITH deduplicated as (
  SELECT *
  FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY date(submission_timestamp), document_id order by submission_timestamp) AS row_number
    FROM `{moz_data_bq_project}.activity_stream_live.impression_stats_v1`
    WHERE submission_timestamp > '{last_timestamp}'
  )
  WHERE row_number = 1
),

impression_data AS (
    SELECT s.*
    FROM deduplicated AS s
    WHERE loaded IS NULL --don't include loaded ping
        AND array_length(tiles) >= 1 --make sure data is valid/non-empty
        AND (source IS NULL or source != 'https://newtab.firefoxchina.cn/newtab/as/activity-stream.html') --exclude custom New Tab page for FX China
        AND (page IS NULL or page != 'https://newtab.firefoxchina.cn/newtab/as/activity-stream.html') --exclude custom New Tab page for FX China
),

flattened_impression_data AS ( --need this step to filter out >2 clicks from a given client on the same tile within 1 second
    SELECT
        UNIX_SECONDS(submission_timestamp) AS submission_timestamp, --truncate timestamp to seconds
        impression_id AS client_id, --client_id renamed to impression_id in GCP
        flattened_tiles.id AS tile_id,
        IFNULL(flattened_tiles.pos, alt_pos) AS position, --the 3x1 layout has a bug where we need to use the position of each element in the tiles array instead of the actual pos field
        source,
        locale,
        normalized_country_code AS country,
        SUM(CASE WHEN click IS NULL AND block IS NULL AND pocket IS NULL THEN 1 ELSE 0 END) AS impressions,
        SUM(CASE WHEN click IS NOT NULL THEN 1 ELSE 0 END) AS clicks,
        SUM(CASE WHEN pocket IS NOT NULL THEN 1 ELSE 0 END) AS pocketed,
        SUM(CASE WHEN block IS NOT NULL THEN 1 ELSE 0 END) AS blocked
    FROM impression_data
    CROSS JOIN UNNEST(impression_data.tiles) AS flattened_tiles
    WITH OFFSET AS alt_pos
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT
    DATE(TIMESTAMP_SECONDS(submission_timestamp)) AS activity_date,
    tile_id,
    position,
    source,
    locale,
    country,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(pocketed) AS pocketed,
    SUM(blocked) AS blocked
FROM flattened_impression_data
WHERE clicks < 3
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 2, 3, 4, 5, 6;
