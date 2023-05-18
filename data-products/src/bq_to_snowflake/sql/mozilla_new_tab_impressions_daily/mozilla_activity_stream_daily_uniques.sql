-- original path/file: dags/mozilla_stats_twice_daily_bq/daily_uniques_export.sql

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
        AND normalized_country_code = 'US'
        AND locale LIKE 'en%'
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
    count(distinct case when a.clicks > 0 THEN a.client_id END) AS user_clicking_cnt,
    count(distinct case when a.impressions > 0 AND a.user_prefs & 4 = 4 and a.user_prefs & 32 = 32 THEN a.client_id END) AS spoc_eligible_users,
    count(distinct case when a.impressions > 0 AND a.user_prefs & 4 = 4 and a.user_prefs & 32 = 32 and t.type = 'spoc' THEN a.client_id END) AS users_viewing_spocs,
    count(distinct case when a.clicks > 0 AND a.user_prefs & 4 = 4 and a.user_prefs & 32 = 32 and t.type = 'spoc' THEN a.client_id END) AS users_clicking_spocs
FROM flattened_impression_data AS a
LEFT JOIN `moz-fx-data-shared-prod.pocket.spoc_tile_ids` AS t
    ON a.tile_id = t.tile_id
WHERE a.clicks < 3
GROUP BY 1
ORDER BY 1;
