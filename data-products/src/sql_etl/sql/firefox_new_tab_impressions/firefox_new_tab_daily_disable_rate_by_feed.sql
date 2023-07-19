WITH
  deduplicated AS (
  SELECT
    *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY DATE(submission_timestamp),
        document_id
      ORDER BY
        submission_timestamp) AS row_number
    FROM
      `moz-fx-data-shared-prod.activity_stream_live.sessions_v1`
      --replace with incremental logic
    WHERE
      TIMESTAMP_TRUNC(submission_timestamp, DAY) = TIMESTAMP("2023-07-18") )
  WHERE
    row_number = 1 )
SELECT
  DATE(submission_timestamp) AS happened_at,
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
  AS feed_name,
  COUNT(DISTINCT client_id) AS users_opening_new_tab_count,
  COUNT(DISTINCT
    CASE
      WHEN user_prefs & 4 != 4 THEN client_id
  END
    ) AS users_with_pocket_disabled_count,
  COUNT(DISTINCT
    CASE
      WHEN user_prefs & 32 != 32 THEN client_id
  END
    ) AS users_with_spocs_disabled_count,
  COUNT(DISTINCT
    CASE
      WHEN user_prefs & 4 = 4 AND user_prefs & 32 = 32 THEN client_id
  END
    ) AS users_eligible_for_spocs_count,
  current_timestamp AS snowflake_loaded_at
FROM
  deduplicated
WHERE
  release_channel = 'release' 
  AND (version LIKE '1%'
    OR version LIKE '2%'
    OR version LIKE '3%'
    OR version LIKE '4%'
    OR version LIKE '5%'
    OR version LIKE '6%'
    OR version LIKE '7%'
    OR version LIKE '8%'
    OR version LIKE '9%'
    OR version LIKE '10%'
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
        AND locale IN ('es-ES')) ))
GROUP BY
  1,
  2
ORDER BY
  1,
  2;