-- original path/file: dags/mozilla_stats_twice_daily_bq/prefs_export.sql

WITH deduplicated as (
  SELECT *
  FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY date(submission_timestamp), document_id order by submission_timestamp) AS row_number
    FROM `{moz_data_bq_project}.activity_stream_live.sessions_v1`
    WHERE submission_timestamp > '{last_timestamp}'
  )
  WHERE row_number = 1
)

select
    DATE(submission_timestamp) AS activity_date,
    count(distinct client_id) AS users,
    count(distinct case when user_prefs & 4 != 4 THEN client_id end) AS pocket_disabled_users,
    count(distinct case when user_prefs & 32 != 32 THEN client_id end) AS spoc_disabled_users,
    count(distinct case when user_prefs & 4 = 4 AND user_prefs & 32 = 32 THEN client_id end) AS spoc_eligible_users
from deduplicated
WHERE normalized_country_code = 'US'
    and locale LIKE 'en%'
    and release_channel = 'release' --this is only beta for now, this is why user numbers are so low
    and (version like '6%' OR version like '7%' OR version like '8%' OR version like '9%' OR version like '10%')
group by 1
order by 1;
