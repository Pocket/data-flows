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
),

all_recent_items as (
    SELECT * FROM recently_scheduled_items
    UNION ALL
    SELECT * FROM recently_updated_items
),

-- Deduplicate based on the CorpusItem id
deduped as (
  SELECT * FROM all_recent_items
  QUALIFY row_number() OVER (PARTITION BY ID ORDER BY REVIEW_TIME DESC) = 1
)

-- Select the n most recent items per topic
SELECT
    ID,
    TOPIC,
    PUBLISHER
FROM deduped
QUALIFY row_number() OVER (PARTITION BY TOPIC ORDER BY REVIEW_TIME DESC) <= %(N_RECS_PER_TOPIC)s;
