SELECT
  DATE(submission_hour) AS happened_at,
  NULL AS recommendation_id,
  ad_id,
  position,
  COUNTIF(interaction_type = 'impression') AS impression_count,
  COUNTIF(interaction_type = 'click') AS click_count,
  0 AS save_count,
  0 AS dismiss_count,
FROM
  `moz-fx-data-shared-prod.ads_derived.interaction_aggregates_hourly_v1`
WHERE
  submission_hour >= '{{ batch_start }}'
  AND submission_hour < '{{ batch_end }}'
  AND form_factor = 'desktop'
  AND placement = 'newtab_spocs'
GROUP BY
  happened_at,
  ad_id,
  position
