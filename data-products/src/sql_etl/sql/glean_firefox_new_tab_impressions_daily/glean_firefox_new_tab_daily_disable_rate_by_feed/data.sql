{% set sql_engine = "bigquery" %}
{% import 'helpers.j2' as helpers with context %}
{% if for_new_offset %}
    select current_timestamp()
{% else %}

--replicates logic of current Prefect query using Glean data
--https://github.com/Pocket/data-flows/blob/main-v2/data-products/src/sql_etl/sql/firefox_new_tab_impressions_daily/firefox_new_tab_daily_disable_rate_by_feed/data.sql

WITH
  deduplicated_pings AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.newtab_v1`
  WHERE
    submission_timestamp >= {{ helpers.parse_iso8601(batch_start) }}
    AND submission_timestamp < {{ helpers.parse_iso8601(batch_end) }} QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE(submission_timestamp),
      document_id
    ORDER BY
      submission_timestamp DESC) = 1 )
SELECT
  DATE(submission_timestamp) AS happened_at,
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
  COUNT(DISTINCT client_info.client_id) AS users_opening_new_tab_count,
  COUNT(DISTINCT
    CASE
      WHEN metrics.boolean.pocket_enabled = FALSE THEN client_info.client_id
  END
    ) AS users_with_pocket_disabled_count,
  COUNT(DISTINCT
    CASE
      WHEN metrics.boolean.pocket_sponsored_stories_enabled = FALSE THEN client_info.client_id
  END
    ) AS users_with_spocs_disabled_count,
  COUNT(DISTINCT
    CASE
      WHEN metrics.boolean.pocket_enabled = TRUE AND metrics.boolean.pocket_sponsored_stories_enabled = TRUE THEN client_info.client_id
  END
    ) AS users_eligible_for_spocs_count,
FROM
  deduplicated_pings
WHERE
  --include only release channel data
  normalized_channel = 'release'
  --include only data from Firefox 121+
  AND SAFE_CAST(SPLIT(client_info.app_display_version, '.')[0] AS int64) >= 121
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
GROUP BY
  1,
  2
ORDER BY
  1
{% endif %}