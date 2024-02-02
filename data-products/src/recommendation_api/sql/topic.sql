WITH prep as (
    SELECT
        approved_corpus_item_external_id as "ID",
        topic as "TOPIC",
        publisher as "PUBLISHER",
        reviewed_corpus_item_updated_at as "REVIEW_TIME"
    FROM "ANALYTICS"."DBT"."APPROVED_CORPUS_ITEMS"
    WHERE REVIEWED_CORPUS_ITEM_UPDATED_AT >= DATEADD('day', -90, current_timestamp())
    AND CORPUS_REVIEW_STATUS = 'recommendation'
    AND TOPIC = %(CORPUS_TOPIC_ID)s
    AND SCHEDULED_SURFACE_ID = %(SCHEDULED_SURFACE_ID)s
    AND NOT is_syndicated
    AND NOT is_collection

    UNION

    SELECT
        approved_corpus_item_external_id as "ID",
        topic as "TOPIC",
        publisher as "PUBLISHER",
        scheduled_corpus_item_scheduled_at as "REVIEW_TIME"
    FROM "ANALYTICS"."DBT"."SCHEDULED_CORPUS_ITEMS"
    WHERE SCHEDULED_CORPUS_ITEM_SCHEDULED_AT BETWEEN DATEADD('day', -90, current_timestamp()) AND current_timestamp()
    AND CORPUS_ITEM_LOADED_FROM = 'MANUAL'
    AND TOPIC = %(CORPUS_TOPIC_ID)s
    AND SCHEDULED_SURFACE_ID = %(SCHEDULED_SURFACE_ID)s
    AND NOT is_syndicated
    AND NOT is_collection

    )

SELECT
    ID,
    TOPIC,
    PUBLISHER
FROM PREP
WHERE id <> 'c931d2f5-0205-48f1-a773-dd0e682977b1'  -- See #incidents on 2023-03-21
QUALIFY row_number() OVER (PARTITION BY ID ORDER BY REVIEW_TIME DESC) = 1
