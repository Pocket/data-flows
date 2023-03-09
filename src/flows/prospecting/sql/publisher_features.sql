WITH last_week_saves AS (
  SELECT
      c.top_domain_name,
      count(distinct c.RESOLVED_URL) as num_items,
      avg(d.save_count * (datediff(day, c.first_parsed_at, d.happened_at))) as avg_save_age,
      sum(d.save_count) as save_count,
      sum(d.open_count) AS open_count,
      sum(d.favorite_count) AS fave_count,
      sum(d.external_share_count) AS share_count
  FROM ANALYTICS.DBT.CONTENT_ENGAGEMENT_BY_DAY_USER_API_ID AS d
  JOIN ANALYTICS.DBT.CONTENT AS c
    ON c.content_id = d.content_id
  LEFT JOIN ANALYTICS.DBT.SCHEDULED_CORPUS_ITEMS as s
    ON s.content_id = c.CONTENT_ID
  WHERE d.happened_at between current_date - 7 and current_date
    AND c.first_parsed_at between current_date - 7 and current_date
    AND c.language = 'en'
    AND c.word_count >= %(MIN_WORD_COUNT)s
    AND d.save_count > 0
    AND c.domain_id <> 5863470  -- getpocket.com
    AND s.CONTENT_ID is NULL  -- no scheduled items
  GROUP BY 1
  ORDER BY save_count DESC
  LIMIT 9000
  ),

topic_distributions AS (
  SELECT
      c.top_domain_name as approved_source,
      SUM(IFF(a.topic = 'BUSINESS', 1, 0)) as num_approved_business,
      SUM(IFF(a.topic = 'CAREER', 1, 0)) as num_approved_career,
      SUM(IFF(a.topic = 'EDUCATION', 1, 0)) as num_approved_education,
      SUM(IFF(a.topic = 'ENTERTAINMENT', 1, 0)) as num_approved_entertainment,
      SUM(IFF(a.topic = 'FOOD', 1, 0)) as num_approved_food,
      SUM(IFF(a.topic = 'GAMING', 1, 0)) as num_approved_gaming,
      SUM(IFF(a.topic = 'HEALTH_FITNESS', 1, 0)) as num_approved_health,
      SUM(IFF(a.topic = 'PARENTING', 1, 0)) as num_approved_parenting,
      SUM(IFF(a.topic = 'PERSONAL_FINANCE', 1, 0)) as num_approved_personal_finance,
      SUM(IFF(a.topic = 'POLITICS', 1, 0)) as num_approved_politics,
      SUM(IFF(a.topic = 'SCIENCE', 1, 0)) as num_approved_science,
      SUM(IFF(a.topic = 'SELF_IMPROVEMENT', 1, 0)) as num_approved_self_improvement,
      SUM(IFF(a.topic = 'TECHNOLOGY', 1, 0)) as num_approved_technology,
      SUM(IFF(a.topic = 'TRAVEL', 1, 0)) as num_approved_travel,
      SUM(IFF(a.topic = 'SPORTS', 1, 0)) as num_approved_sports,
      SUM(IFF(a.topic IS NOT NULL AND a.topic <> 'CORONAVIRUS', 1, 0)) as num_approved
    FROM content as c
    JOIN analytics.dbt.approved_corpus_items as a
      ON a.content_id = c.content_id
    WHERE a.topic is not NULL
      AND a.reviewed_corpus_item_created_at > current_date - %(MAX_APPROVED_AGE)s
    GROUP BY 1
    ORDER BY num_approved DESC
    LIMIT 9000
  )

  SELECT
    c.top_domain_name,
    t.*,
    c.save_count as total_save_count,
    c.avg_save_age,
    c.save_count / c.num_items as avg_save_count,
    c.num_items as num_items_saved,
    LN(c.SAVE_COUNT) as log_total_saves,
    current_timestamp as updated_at
  FROM last_week_saves as c
  LEFT JOIN topic_distributions as t
    ON c.top_domain_name = t.approved_source