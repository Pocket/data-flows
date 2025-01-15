WITH uapi_interactions_deduped AS (
  SELECT
    DATE(submission_timestamp) AS happened_at,
    NULL AS recommendation_id,
    COALESCE(SAFE_CAST(metrics.string.ad_id AS int), -1) AS ad_id,
    COALESCE(SAFE_CAST(metrics.string.ad_client_position AS int), -1) AS position,
    metrics.string.ad_interaction AS interaction_type,
  FROM
    {% if with_stable %}
    `moz-fx-data-shared-prod.ads_backend_stable.interaction_v1` AS i
    {% else %}
    `moz-fx-data-shared-prod.ads_backend_live.interaction_v1` AS i
    {% endif %}
  WHERE 
    submission_timestamp >= '{{ batch_start }}'
    AND submission_timestamp < '{{ batch_end }}'
    AND metrics.string.ad_client_form_factor = 'desktop'
    AND metrics.string.ad_client_placement = 'newtab_spocs'
  QUALIFY 
    ROW_NUMBER() OVER (
      PARTITION BY DATE(submission_timestamp), document_id
      ORDER BY submission_timestamp DESC
  ) = 1
)
SELECT
  happened_at,
  recommendation_id,
  ad_id,
  position,
  SUM(
    CASE
      WHEN interaction_type = 'impression' THEN 1
      ELSE 0
    END
  ) AS impression_count,
  SUM(
    CASE
      WHEN interaction_type = 'click' THEN 1
      ELSE 0
    END
  ) AS click_count,
  0 AS save_count,
  0 AS dismiss_count,
FROM uapi_interactions_deduped
GROUP BY
  1,
  2,
  3,
  4