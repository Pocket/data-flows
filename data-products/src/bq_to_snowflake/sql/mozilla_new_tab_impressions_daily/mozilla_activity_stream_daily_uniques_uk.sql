-- original path/file: dags/mozilla_stats_twice_daily_bq/daily_uniques_export_uk.sql

WITH deduplicated as (
  SELECT *
  FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY date(submission_timestamp), document_id order by submission_timestamp) AS row_number
    FROM `moz-fx-data-shared-prod.activity_stream_live.impression_stats_v1`
    WHERE EXTRACT(date FROM submission_timestamp) BETWEEN DATE_ADD('{{ ds }}', INTERVAL -1 DAY) AND '{{ ds }}'
  )
  WHERE row_number = 1
),

impression_data AS (
    SELECT s.*
    FROM deduplicated AS s
    WHERE loaded IS NULL --don't include loaded ping
        AND array_length(tiles) >= 1 --make sure data is valid/non-empty
        AND locale = 'en-GB'
        AND release_channel = 'release'
),

flattened_impression_data AS ( --need this step to filter out >2 clicks from a given client on the same tile within 1 second
    SELECT
        UNIX_SECONDS(submission_timestamp) AS submission_timestamp, --truncate timestamp to seconds
        impression_id AS client_id, --client_id renamed to impression_id in GCP
        user_prefs,
        flattened_tiles.id AS tile_id,
        IFNULL(flattened_tiles.pos, alt_pos) AS position, --the 3x1 layout has a bug where we need to use the position of each element in the tiles array instead of the actual pos field
        SUM(CASE WHEN click IS NULL AND block IS NULL AND pocket IS NULL THEN 1 ELSE 0 END) AS impressions,
        SUM(CASE WHEN click IS NOT NULL THEN 1 ELSE 0 END) AS clicks,
        SUM(CASE WHEN pocket IS NOT NULL THEN 1 ELSE 0 END) AS pocketed,
        SUM(CASE WHEN block IS NOT NULL THEN 1 ELSE 0 END) AS blocked
    FROM impression_data
    CROSS JOIN UNNEST(impression_data.tiles) AS flattened_tiles
    WITH OFFSET AS alt_pos
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    DATE(TIMESTAMP_SECONDS(a.submission_timestamp)) AS activity_date,
    count(distinct case when a.impressions > 0 THEN a.client_id END) AS user_viewing_cnt,
    count(distinct case when a.clicks > 0 THEN a.client_id END) AS user_clicking_cnt
FROM flattened_impression_data AS a
WHERE a.clicks < 3
GROUP BY 1
ORDER BY 1;
