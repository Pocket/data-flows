WITH latest_approvals AS (

    SELECT
       content_id,
       prospect_source as TYPE,
       'approved' as STATUS,  -- collapsing recommendation and corpus to 'approved' as in backfill
       DATE(reviewed_corpus_item_created_at) AS DATE_ADDED
    FROM analytics.dbt.approved_corpus_items  -- model with curator approved items
    WHERE loaded_from = 'PROSPECT'
    AND scheduled_surface_id = %(PROSPECT_SURFACE_GUID)s  -- (e.g. en-US/scheduled_surface_id='NEW_TAB_EN_US')
    AND DATEDIFF(day, reviewed_corpus_item_created_at, current_date) <= %(MAX_INC_AGE)s
    qualify row_number() over (partition by content_id order by reviewed_corpus_item_created_at desc) = 1
),

-- manual submissions to new tab by curators don't have prospect source information which
-- currently comes from prospect candidate set model.
latest_manuals AS (

    SELECT
       content_id,
       corpus_item_loaded_from as TYPE,
       'approved' as STATUS,  -- collapsing recommendation and corpus to 'approved' as in backfill
       DATE(reviewed_corpus_item_created_at) AS DATE_ADDED
    FROM analytics.dbt.scheduled_corpus_items  -- manually submitted items are overwhelmingly scheduled
    WHERE corpus_item_loaded_from = 'MANUAL'   -- remaining scheduled items are 'recommended'
    AND scheduled_surface_id = %(PROSPECT_SURFACE_GUID)s -- (e.g. en-US/scheduled_surface_id='NEW_TAB_EN_US')
    AND DATEDIFF(day, reviewed_corpus_item_created_at, current_date) <= %(MAX_INC_AGE)s -- 1 day
    qualify row_number() over (partition by content_id order by reviewed_corpus_item_created_at desc) = 1

),

latest_rejects AS (

    SELECT
       content_id,
       prospect_source as TYPE,
       'denied' as STATUS, -- this is legacy label for rejected prospects to be consistent with backfill
       DATE(reviewed_corpus_item_created_at) AS DATE_ADDED
    FROM analytics.dbt.rejected_corpus_items  -- model with historic curator decisions
    WHERE scheduled_surface_id = %(PROSPECT_SURFACE_GUID)s  -- (e.g. en-US/scheduled_surface_id='NEW_TAB_EN_US')
    AND DATEDIFF(day, reviewed_corpus_item_created_at, current_date) <= %(MAX_INC_AGE)s
    qualify row_number() over (partition by content_id order by reviewed_corpus_item_created_at desc) = 1

),

latest_curator_decisions as (
    select * from latest_approvals
    union
    select * from latest_rejects
    union
    select * from latest_manuals
),


action_counts as (
    SELECT
       a.CONTENT_ID,
       a.RESOLVED_ID,
       a.DOMAIN_ID,
       a.TOP_DOMAIN_NAME AS PUBLISHER,
       a.WORD_COUNT,
       a.RESOLVED_URL,
       a.TITLE,
       p.TYPE,
       p.STATUS,
       p.DATE_ADDED,
       SUM(CASE WHEN (p.DATE_ADDED - a.HAPPENED_AT) BETWEEN 0 AND 1
           THEN a.SAVE_COUNT ELSE 0 END) AS DAY1_SAVE_COUNT,
       SUM(CASE WHEN (p.DATE_ADDED - a.HAPPENED_AT) = 2
           THEN a.SAVE_COUNT ELSE 0 END) AS DAY2_SAVE_COUNT,
       SUM(CASE WHEN (p.DATE_ADDED - a.HAPPENED_AT) = 3
           THEN a.SAVE_COUNT ELSE 0 END) AS DAY3_SAVE_COUNT,
       SUM(CASE WHEN (p.DATE_ADDED - a.HAPPENED_AT) = 4
           THEN a.SAVE_COUNT ELSE 0 END) AS DAY4_SAVE_COUNT,
       SUM(CASE WHEN (p.DATE_ADDED - a.HAPPENED_AT) = 5
           THEN a.SAVE_COUNT ELSE 0 END) AS DAY5_SAVE_COUNT,
       SUM(CASE WHEN (p.DATE_ADDED - a.HAPPENED_AT) = 6
           THEN a.SAVE_COUNT ELSE 0 END) AS DAY6_SAVE_COUNT,
       SUM(CASE WHEN (p.DATE_ADDED - a.HAPPENED_AT) = 7
           THEN a.SAVE_COUNT ELSE 0 END) AS DAY7_SAVE_COUNT,
       SUM(a.SAVE_COUNT)                          AS SAVE_COUNT,
       SUM(a.OPEN_COUNT)                          AS OPEN_COUNT,
       SUM(a.EXTERNAL_SHARE_COUNT)                AS SHARE_COUNT,
       SUM(a.favorite_count)                      AS FAVORITE_COUNT
    FROM ANALYTICS.DBT.CONTENT_ENGAGEMENT_BY_DAY_USER_API_ID as a
    JOIN latest_curator_decisions as p
      ON p.content_id = a.content_id
    WHERE (p.DATE_ADDED - a.HAPPENED_AT) BETWEEN 0 AND 7
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),

max_counts_by_domain AS (
    SELECT
       d.DOMAIN_ID,
       d.PUBLISHER,
       max(d.SAVE_COUNT) AS max_save_count,
       max(d.OPEN_COUNT) AS max_open_count
    FROM action_counts AS d
    GROUP BY 1, 2
)

SELECT
    c.*,
    d.max_save_count AS MAX_DOMAIN_SAVES,
    d.max_open_count AS MAX_DOMAIN_OPENS
 FROM action_counts AS c
 JOIN max_counts_by_domain AS d
   ON d.DOMAIN_ID = c.DOMAIN_ID
WHERE c.OPEN_COUNT < c.SAVE_COUNT
  AND c.publisher <> 'getpocket.com'