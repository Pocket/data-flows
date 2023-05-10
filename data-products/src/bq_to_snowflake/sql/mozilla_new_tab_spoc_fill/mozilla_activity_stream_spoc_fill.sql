-- original path/file: dags/dynamic/bq_to_snowflake/mozilla_new_tab_spoc_fill/mozilla_activity_stream_spoc_fill.sql

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
        AND click IS NULL AND block IS NULL AND pocket IS NULL --impressions only
        AND normalized_country_code = 'US'
        AND locale LIKE 'en%'
        AND release_channel = 'release'
        AND user_prefs & 4 = 4
        AND user_prefs & 32 = 32
),

flattened_impression_data AS (
    SELECT
        UNIX_SECONDS(submission_timestamp) AS submission_timestamp, --truncate timestamp to seconds
        flattened_tiles.id AS tile_id,
        IFNULL(flattened_tiles.pos, alt_pos) AS position, --the 3x1 layout has a bug where we need to use the position of each element in the tiles array instead of the actual pos field
        SUM(1) AS impressions
    FROM impression_data
    CROSS JOIN UNNEST(impression_data.tiles) AS flattened_tiles
    WITH OFFSET AS alt_pos
    GROUP BY 1, 2, 3
),

new_tab_impressions AS ( --use impressions of first position as a proxy for # of new tab opens
    SELECT
        DATE(TIMESTAMP_SECONDS(submission_timestamp)) AS activity_date,
        SUM(impressions) AS new_tab_impressions
    FROM flattened_impression_data
    WHERE position = 0
    GROUP BY 1
)

SELECT
    DATE(TIMESTAMP_SECONDS(a.submission_timestamp)) AS activity_date,
    a.position,
    MIN(n.new_tab_impressions) AS new_tab_impressions,
    SUM(a.impressions) AS position_impressions,
    SUM(a.impressions) / MIN(n.new_tab_impressions) AS visibility,
    SUM(CASE WHEN t.type = 'spoc' THEN a.impressions END) AS spoc_impressions,
    SUM(CASE WHEN t.type = 'spoc' THEN a.impressions END) / SUM(a.impressions) AS fill_rate
FROM flattened_impression_data AS a
LEFT JOIN `moz-fx-data-shared-prod.pocket.spoc_tile_ids` AS t
    ON a.tile_id = t.tile_id
JOIN new_tab_impressions AS n
  ON n.activity_date = DATE(TIMESTAMP_SECONDS(a.submission_timestamp))
WHERE a.position IN (2, 4, 11, 20)
  --starting 2022-03-08, SPOC positions will change
  --add as a separate OR statement to make it easier to drop the old SPOC positions once we have critical mass on the new version
  or a.position IN (1, 5, 7, 11, 18, 20)
GROUP BY 1, 2
ORDER BY 1, 2;
