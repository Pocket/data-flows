SELECT
    APPROVED_CORPUS_ITEM_EXTERNAL_ID as "ID",
    TOPIC as "TOPIC",
    PUBLISHER as "PUBLISHER"
FROM "ANALYTICS"."DBT"."SCHEDULED_CORPUS_ITEMS"
WHERE SCHEDULED_SURFACE_ID = %(SCHEDULED_SURFACE_ID)s
AND approved_corpus_item_external_id <> 'c931d2f5-0205-48f1-a773-dd0e682977b1'  -- See #incidents on 2023-03-21
-- Pocket Hits content should be available on Home at 3am EST = 7am UTC. Override the scheduled time to 7am.
-- `DATE_TRUNC` truncates the time, and `DATEADD` adds 7 hours to set the time to 7am UTC. This corresponds to 3am EST.
AND DATEADD('hour', 7, DATE_TRUNC('DAY', SCHEDULED_CORPUS_ITEM_SCHEDULED_AT)) < CURRENT_TIMESTAMP
QUALIFY row_number() OVER (PARTITION BY APPROVED_CORPUS_ITEM_EXTERNAL_ID ORDER BY SCHEDULED_CORPUS_ITEM_SCHEDULED_AT DESC) = 1
ORDER BY SCHEDULED_CORPUS_ITEM_SCHEDULED_AT DESC
LIMIT 8  -- Only include past Pocket Hits stories if today's aren't available. There are 8 stories per email.
