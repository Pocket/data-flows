WITH latest_approvals AS (

    SELECT
       content_id,
       prospect_source as TYPE,
       'approved' as STATUS,  -- collapsing recommendation and corpus to 'approved' as in backfill
       DATE(reviewed_corpus_item_created_at) AS DATE_ADDED
    FROM analytics.dbt.approved_corpus_items  -- model with historic curator decisions
    WHERE loaded_from = 'PROSPECT'
    AND scheduled_surface_id = %(PROSPECT_SURFACE_GUID)s  -- (e.g. en-US/scheduled_surface_id='NEW_TAB_EN_US')
    AND reviewed_corpus_item_created_at >  %(SURFACE_START_DATE)s  -- date when surface started in curation tool
    qualify row_number() over (partition by content_id order by reviewed_corpus_item_created_at desc) = 1
),

-- manual submissions to new tab by curators don't have prospect source information which
-- currently comes from prospect candidate set model.  we need a scheduled surface which is
-- only available in the scheduled_corpus_items model for those manual submissions that are
-- scheduled (in addition to having status recommended)
latest_manuals AS (

    SELECT
       content_id,
       corpus_item_loaded_from as TYPE,
       'approved' as STATUS,  -- collapsing recommendation and corpus to 'approved' as in backfill
       DATE(reviewed_corpus_item_created_at) AS DATE_ADDED
    FROM analytics.dbt.scheduled_corpus_items  -- model with historic curator decisions
    WHERE corpus_item_loaded_from = 'MANUAL'
    AND scheduled_surface_id = %(PROSPECT_SURFACE_GUID)s -- (e.g. en-US/scheduled_surface_id='NEW_TAB_EN_US')
    AND reviewed_corpus_item_created_at >  %(SURFACE_START_DATE)s  -- date when surface started in curation tool
    qualify row_number() over (partition by content_id order by reviewed_corpus_item_created_at desc) = 1

),


-- one year of prospects (rejected and approved) prior to the curation tool
-- NEW_TAB_EN_US migrated to curation tool on 5/31/22
-- this model includes historical approvals and rejections, airflow model only has approvals
-- reviewed_corpus_item_prospects_merged doesn't include manual submissions
backfill_prospects AS (

    SELECT
        c.content_id,
        p.type,
        p.status,
        DATE (p.time_added) AS DATE_ADDED
    FROM analytics.dbt_staging.stg_curated_feed_prospects as p
    JOIN analytics.dbt.content as c
      ON c.resolved_id = p.resolved_id
    WHERE p.feed_id = %(PROSPECT_LEGACY_FEED_ID) s
      AND DATEDIFF(day, p.time_added, %(SURFACE_START_DATE)s ) BETWEEN 0 AND %(MAX_BACKFILL_AGE)s

),

latest_rejects AS (

    SELECT
       content_id,
       prospect_source as TYPE,
       'denied' as STATUS, -- this is legacy label for rejected prospects to be consistent with backfill
       DATE(reviewed_corpus_item_created_at) AS DATE_ADDED
    FROM analytics.dbt.rejected_corpus_items  -- model with historic curator decisions
    WHERE scheduled_surface_id = %(PROSPECT_SURFACE_GUID)s  -- (e.g. en-US/scheduled_surface_id='NEW_TAB_EN_US')
    AND reviewed_corpus_item_created_at >= %(SURFACE_START_DATE)s  -- date when surface started in curation tool
    qualify row_number() over (partition by content_id order by reviewed_corpus_item_created_at desc) = 1

),

latest_curator_decisions as (
    select * from latest_approvals
    union
    select * from latest_rejects
    union
    select * from backfill_prospects
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
           THEN a.SAVE_COUNT ELSE 0 END) AS day1_save_count,
       SUM(CASE WHEN (p.DATE_ADDED - a.HAPPENED_AT) = 2
           THEN a.SAVE_COUNT ELSE 0 END) AS day2_save_count,
       SUM(CASE WHEN (p.DATE_ADDED - a.HAPPENED_AT) = 3
           THEN a.SAVE_COUNT ELSE 0 END) AS day3_save_count,
       SUM(CASE WHEN (p.DATE_ADDED - a.HAPPENED_AT) = 4
           THEN a.SAVE_COUNT ELSE 0 END) AS day4_save_count,
       SUM(CASE WHEN (p.DATE_ADDED - a.HAPPENED_AT) = 5
           THEN a.SAVE_COUNT ELSE 0 END) AS day5_save_count,
       SUM(CASE WHEN (p.DATE_ADDED - a.HAPPENED_AT) = 6
           THEN a.SAVE_COUNT ELSE 0 END) AS day6_save_count,
       SUM(CASE WHEN (p.DATE_ADDED - a.HAPPENED_AT) = 7
           THEN a.SAVE_COUNT ELSE 0 END) AS day7_save_count,
       SUM(a.SAVE_COUNT)                          AS save_count,
       SUM(a.OPEN_COUNT)                          AS open_count,
       SUM(a.EXTERNAL_SHARE_COUNT)                AS share_count,
       SUM(a.favorite_count)                      AS favorite_count
    FROM ANALYTICS.DBT.CONTENT_ENGAGEMENT_BY_DAY_USER_API_ID as a
    JOIN latest_curator_decisions as p
      ON p.content_id = a.content_id
    WHERE (p.DATE_ADDED - a.HAPPENED_AT) BETWEEN 0 AND 7
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),

max_counts_by_domain AS (
    SELECT
       d.domain_id,
       d.publisher,
       max(d.save_count) AS max_save_count,
       max(d.open_count) AS max_open_count
    FROM action_counts AS d
    GROUP BY 1, 2
)

SELECT
    c.*,
    d.max_save_count AS max_domain_saves,
    d.max_open_count AS max_domain_opens
 FROM action_counts AS c
 JOIN max_counts_by_domain AS d
   ON d.domain_id = c.domain_id
WHERE c.open_count < c.save_count
  AND c.publisher <> 'getpocket.com'