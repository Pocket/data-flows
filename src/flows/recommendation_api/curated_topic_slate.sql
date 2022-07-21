SELECT
    s.resolved_id,
    c.top_domain_name as publisher,
    s.resolved_url as url,
    s.topic,
    s.approved_corpus_item_external_id as id
FROM scheduled_corpus_items as s
JOIN content as c
  ON c.content_id = s.content_id
WHERE s.TOPIC = %(TOPIC)s
  AND s.scheduled_surface_id = %(SCHEDULED_SURFACE_ID)s
ORDER BY s.scheduled_corpus_item_scheduled_at DESC
LIMIT %(NUM_CANDIDATES)s
