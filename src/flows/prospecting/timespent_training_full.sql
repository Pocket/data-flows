
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
        DATE(p.time_added) AS DATE_ADDED
    FROM analytics.dbt_staging.stg_curated_feed_prospects as p
    JOIN analytics.dbt.content as c
      on c.resolved_id = p.resolved_id
    WHERE p.feed_id = %(PROSPECT_LEGACY_FEED_ID)s
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
       a.content_id,
       a.RESOLVED_ID,
       a.DOMAIN_ID,
       a.TOP_DOMAIN_NAME AS PUBLISHER,
       a.WORD_COUNT,
       a.RESOLVED_URL,
       a.TITLE,
       p.TYPE,
       p.STATUS,
       p.DATE_ADDED,
       SUM(a.SAVE_COUNT)                          AS save_count,
       SUM(a.OPEN_COUNT)                          AS open_count,
       SUM(a.EXTERNAL_SHARE_COUNT)                AS share_count,
       SUM(a.favorite_count)                      AS favorite_count
    FROM ANALYTICS.DBT.CONTENT_ENGAGEMENT_BY_DAY as a
    JOIN latest_curator_decisions as p
      ON p.content_id = a.content_id
    WHERE a.HAPPENED_AT between p.DATE_ADDED - 7 and p.DATE_ADDED
    and a.resolved_id <> 3242033017  -- pocket welcome page
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),

-- session time is coming from a pageview model and needs to be aggregated by the URL
-- this only includes time in app on mobile hence the URL associated with reader view
time_spent_prep as (
  select
    r.content_id,
    r.resolved_id,
    COUNT(1) AS pageviews,
    SUM(CASE WHEN (r.DATE_ADDED - DATE(a.began_at)) BETWEEN 0 AND 1
        THEN a.TIME_ENGAGED_IN_S ELSE 0 END) AS day1_timespent,
    SUM(CASE WHEN (r.DATE_ADDED - DATE(a.began_at)) = 2
        THEN a.TIME_ENGAGED_IN_S ELSE 0 END) AS day2_timespent,
    SUM(CASE WHEN (r.DATE_ADDED - DATE(a.began_at)) = 3
        THEN a.TIME_ENGAGED_IN_S ELSE 0 END) AS day3_timespent,
    SUM(CASE WHEN (r.DATE_ADDED - DATE(a.began_at)) = 4
        THEN a.TIME_ENGAGED_IN_S ELSE 0 END) AS day4_timespent,
    SUM(CASE WHEN (r.DATE_ADDED - DATE(a.began_at)) = 5
        THEN a.TIME_ENGAGED_IN_S ELSE 0 END) AS day5_timespent,
    SUM(CASE WHEN (r.DATE_ADDED - DATE(a.began_at)) = 6
        THEN a.TIME_ENGAGED_IN_S ELSE 0 END) AS day6_timespent,
    SUM(CASE WHEN (r.DATE_ADDED - DATE(a.began_at)) = 7
        THEN a.TIME_ENGAGED_IN_S ELSE 0 END) AS day7_timespent,
    SUM(CASE WHEN (r.DATE_ADDED - DATE(a.began_at)) BETWEEN 0 AND 1
        THEN 1 ELSE 0 END) AS day1_pageviews,
    SUM(CASE WHEN (r.DATE_ADDED - DATE(a.began_at)) = 2
        THEN 1 ELSE 0 END) AS day2_pageviews,
    SUM(CASE WHEN (r.DATE_ADDED - DATE(a.began_at)) = 3
        THEN 1 ELSE 0 END) AS day3_pageviews,
    SUM(CASE WHEN (r.DATE_ADDED - DATE(a.began_at)) = 4
        THEN 1 ELSE 0 END) AS day4_pageviews,
    SUM(CASE WHEN (r.DATE_ADDED - DATE(a.began_at)) = 5
        THEN 1 ELSE 0 END) AS day5_pageviews,
    SUM(CASE WHEN (r.DATE_ADDED - DATE(a.began_at)) = 6
        THEN 1 ELSE 0 END) AS day6_pageviews,
    SUM(CASE WHEN (r.DATE_ADDED - DATE(a.began_at)) = 7
        THEN 1 ELSE 0 END) AS day7_pageviews,
    SUM(a.TIME_ENGAGED_IN_S) as time_spent_sum
  from ANALYTICS.DBT.SNOWPLOW_PAGE_VIEWS as a
  join ANALYTICS.DBT_STAGING.STG_LEGACY_ITEM_ID_TO_GIVEN_URL_MAP as b
    on TRY_CAST(SPLIT(a.PAGE_URL_PATH, '/')[2]::string as integer) = b.ITEM_ID
  join action_counts as r
    on b.resolved_id = r.resolved_id
  where a.PAGE_URL_PATH like '/read/%%' -- this is how to escape the percent wildcard used for like in a parameterized query
    and a.TIME_ENGAGED_IN_S > 30        -- more info here https://stackoverflow.com/questions/48777070/using-in-like-clause-of-select-statement-in-python
    -- read time in seconds is estimated using a reading speed of 265 wpm
    and a.TIME_ENGAGED_IN_S < (r.word_count::float / 265) * 60 * 1.8  -- drop sessions that are too long
    and r.date_added BETWEEN DATE(a.began_at) AND DATE(a.began_at) + 7
    and r.resolved_id <> 3242033017  -- pocket welcome page
  group by 1, 2
  )

select
  p.*,
  t.pageviews,
  t.day1_timespent,
  t.day2_timespent,
  t.day3_timespent,
  t.day4_timespent,
  t.day5_timespent,
  t.day6_timespent,
  t.day7_timespent,
  t.day1_pageviews,
  t.day2_pageviews,
  t.day3_pageviews,
  t.day4_pageviews,
  t.day5_pageviews,
  t.day6_pageviews,
  t.day7_pageviews,
  t.time_spent_sum as total_timespent
from action_counts as p
join time_spent_prep as t
  on p.content_id = t.content_id
