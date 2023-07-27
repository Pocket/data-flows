{% set sql_engine = "bigquery" %}
{% import 'helpers.j2' as helpers with context %}
{% if for_new_offset %}
    select current_timestamp()
{% else %}
WITH
  deduplicated AS (
    SELECT
      *   
    FROM
      {% if for_backfill %}
      `moz-fx-data-shared-prod.activity_stream_stable.sessions_v1`
    {% else %}
      `moz-fx-data-shared-prod.activity_stream_live.sessions_v1`  
    {% endif %}
    WHERE submission_timestamp >= {{ helpers.parse_iso8601(batch_start) }}
    AND submission_timestamp < {{ helpers.parse_iso8601(batch_end) }}
    QUALIFY row_number() over (PARTITION BY DATE(submission_timestamp),
    document_id
    ORDER BY
    submission_timestamp desc) = 1
)
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
FROM
  deduplicated
WHERE
  release_channel = 'release' --this is only beta for now, this is why user numbers are so low
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
  1;
{% endif %}