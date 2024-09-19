-- this script is only called for NEW_TAB_EN_US, so we can hard-code the timezone and offset below

-- get the current timestamp in EST
SET max_timestamp = CONVERT_TIMEZONE('UTC', 'America/New_York', current_timestamp());

-- adjust for 3am offset for en-US new tab content rollover
SET max_timestamp = dateadd(hour, -3, $max_timestamp);

WITH recently_updated_items as (
    SELECT
        approved_corpus_item_external_id as "ID",
        topic as "TOPIC",
        publisher as "PUBLISHER",
        reviewed_corpus_item_updated_at as "REVIEW_TIME",
        -- prefer scheduled items over approved items
        2 as "RELEVANCE"
    FROM "ANALYTICS"."DBT"."APPROVED_CORPUS_ITEMS"
    WHERE CORPUS_REVIEW_STATUS = 'recommendation'
    -- only pull corpus items that were reviewed before the max_timestamp
    AND REVIEWED_CORPUS_ITEM_CREATED_AT < $max_timestamp
    AND SCHEDULED_SURFACE_ID = %(SCHEDULED_SURFACE_ID)s
    AND NOT is_syndicated
    AND NOT is_collection
    AND approved_corpus_item_external_id not in (
        'c931d2f5-0205-48f1-a773-dd0e682977b1',  -- See #incidents on 2023-03-21
        'd5edaef4-fa6c-4293-934e-33d4db207ddd' -- And again on 2024-09-19
    )
),

recently_scheduled_items as (
    SELECT
        approved_corpus_item_external_id as "ID",
        topic as "TOPIC",
        publisher as "PUBLISHER",
        scheduled_corpus_item_scheduled_at as "REVIEW_TIME",
        -- prefer scheduled items over approved items
        1 as "RELEVANCE"
    FROM "ANALYTICS"."DBT"."SCHEDULED_CORPUS_ITEMS"
    -- only pull scheduled items that are scheduled before the max_timestamp
    WHERE SCHEDULED_CORPUS_ITEM_SCHEDULED_AT < $max_timestamp
    AND SCHEDULED_SURFACE_ID = %(SCHEDULED_SURFACE_ID)s
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
WHERE TOPIC is not null
AND PUBLISHER is not null
QUALIFY row_number() OVER (PARTITION BY TOPIC ORDER BY RELEVANCE, REVIEW_TIME DESC) <= %(N_RECS_PER_TOPIC)s;
