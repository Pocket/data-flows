WITH recently_updated_items as (
    SELECT
        approved_corpus_item_external_id as "ID",
        topic as "TOPIC",
        publisher as "PUBLISHER",
        reviewed_corpus_item_updated_at as "REVIEW_TIME"
    FROM "ANALYTICS"."DBT"."APPROVED_CORPUS_ITEMS"
    WHERE CORPUS_REVIEW_STATUS = 'recommendation'
    AND SCHEDULED_SURFACE_ID = 'NEW_TAB_EN_US'
    AND NOT is_syndicated
    AND NOT is_collection
),

recently_scheduled_items as (
    SELECT
        approved_corpus_item_external_id as "ID",
        topic as "TOPIC",
        publisher as "PUBLISHER",
        scheduled_corpus_item_scheduled_at as "REVIEW_TIME"
    FROM "ANALYTICS"."DBT"."SCHEDULED_CORPUS_ITEMS"
    WHERE SCHEDULED_CORPUS_ITEM_SCHEDULED_AT < current_timestamp()
    AND CORPUS_ITEM_LOADED_FROM = 'MANUAL'  -- should this be removed?
    AND SCHEDULED_SURFACE_ID = 'NEW_TAB_EN_US'
    AND NOT is_syndicated
    AND NOT is_collection
)

SELECT
    "ID",
    "TOPIC",
    "PUBLISHER"
FROM
    (SELECT * FROM recently_scheduled_items
     UNION ALL
     SELECT * FROM recently_updated_items)
WHERE TOPIC IN (%(CORPUS_TOPIC_LIST)s)
AND REVIEW_TIME >= current_date() - %(MAX_AGE_DAYS)s
QUALIFY row_number() OVER (PARTITION BY ID ORDER BY REVIEW_TIME DESC) = 1
ORDER BY REVIEW_TIME DESC
