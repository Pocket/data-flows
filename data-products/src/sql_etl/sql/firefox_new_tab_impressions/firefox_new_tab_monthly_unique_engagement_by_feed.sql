WITH
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
    `moz-fx-data-shared-prod.activity_stream.impression_stats` AS s
    --replace with incremental logic
  WHERE
    TIMESTAMP_TRUNC(submission_timestamp, DAY) > TIMESTAMP("2022-12-31")
    AND loaded IS NULL --don't include loaded ping
    AND ARRAY_LENGTH(tiles) >= 1 --make sure data is valid/non-empty
    AND release_channel = 'release'
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
  flattened_impression_data AS ( --need this step to filter out >2 clicks from a given client on the same tile within 1 second
  SELECT
    UNIX_SECONDS(submission_timestamp) AS submission_timestamp,
    feed_name,
    --truncate timestamp to seconds
    impression_id AS client_id,
    --client_id renamed to impression_id in GCP
    user_prefs,
    flattened_tiles.id AS tile_id,
    IFNULL(flattened_tiles.pos, alt_pos) AS position,
    --the 3x1 layout has a bug where we need to use the position of each element in the tiles array instead of the actual pos field
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
    6 )
SELECT
  DATE_TRUNC(DATE(TIMESTAMP_SECONDS(a.submission_timestamp)), month) AS happened_at,
  feed_name,
  COUNT(DISTINCT
    CASE
      WHEN a.impressions > 0 THEN a.client_id
  END
    ) AS users_viewing_recs_count,
  COUNT(DISTINCT
    CASE
      WHEN a.clicks > 0 THEN a.client_id
  END
    ) AS users_clicking_recs_count,
  COUNT(DISTINCT
    CASE
      WHEN a.impressions > 0 AND a.user_prefs & 4 = 4 AND a.user_prefs & 32 = 32 THEN a.client_id
  END
    ) AS users_eligible_for_spocs_count,
  COUNT(DISTINCT
    CASE
      WHEN a.impressions > 0 AND a.user_prefs & 4 = 4 AND a.user_prefs & 32 = 32 AND t.type = 'spoc' THEN a.client_id
  END
    ) AS users_viewing_spocs_count,
  COUNT(DISTINCT
    CASE
      WHEN a.clicks > 0 AND a.user_prefs & 4 = 4 AND a.user_prefs & 32 = 32 AND t.type = 'spoc' THEN a.client_id
  END
    ) AS users_clicking_spocs_count,
  current_timestamp AS snowflake_loaded_at
FROM
  flattened_impression_data AS a
LEFT JOIN
  `moz-fx-data-shared-prod.pocket.spoc_tile_ids` AS t
ON
  a.tile_id = t.tile_id
WHERE
  a.clicks < 3
GROUP BY
  1,
  2
ORDER BY
  1,
  2;