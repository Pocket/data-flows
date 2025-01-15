SELECT
  DATE(i.submission_timestamp) AS happened_at,
  NULL AS recommendation_id,
  COALESCE(SAFE_CAST(i.metrics.string.ad_id AS int), -1) AS tile_id,
  COALESCE(SAFE_CAST(i.metrics.string.ad_client_position AS int), -1) AS position,
  SUM(CASE
      WHEN i.metrics.string.ad_interaction = 'impression' THEN 1
      ELSE 0
  END
    ) AS impression_count,
  SUM(CASE
      WHEN i.metrics.string.ad_interaction = 'click' THEN 1
      ELSE 0
  END
    ) AS click_count,
  0 AS save_count,
  0 AS dismiss_count
FROM
  ads_backend.interaction AS i
WHERE 
    submission_timestamp >= '{{ batch_start }}'
    AND submission_timestamp < '{{ batch_end }}'
GROUP BY
  1,
  2,
  3,
  4