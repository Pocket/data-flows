{% set sql_engine = "snowflake" %}
with top_domains AS (
    select DOMAIN_NAME, sum(SAVE_COUNT) as cnt 
    from ANALYTICS.DBT.DOMAIN_ENGAGEMENT_BY_DAY
    where HAPPENED_AT BETWEEN current_timestamp - interval '30 days' AND current_timestamp
    group by DOMAIN_NAME
    order by cnt desc
    limit 5000
), these_items AS (
  select
    RESOLVED_ID,
    happened_at,
    RESOLVED_URL,
    sum(save_count) as save_count,
    sum(open_count) as open_count,
    sum(favorite_count) as favorite_count,
    sum(external_share_count) as share_count
  from ANALYTICS.DBT.CONTENT_ENGAGEMENT_BY_DAY
  where happened_at BETWEEN current_timestamp - interval '24 hours' AND current_timestamp
    and language = 'en'
    and is_article = 1
    and TOP_DOMAIN_NAME IN (
        select DOMAIN_NAME
        from top_domains
    )
  group by 1,2,3
)

select
  resolved_id,
  happened_at,
  resolved_url,
  save_count,
  open_count,
  favorite_count,
  share_count,
  analytics.dbt.impact_score(save_count, open_count, favorite_count, share_count) as impact_score,
  save_count * analytics.dbt.impact_score(save_count, open_count, favorite_count, share_count) as blended_score
  from these_items
  order by happened_at asc