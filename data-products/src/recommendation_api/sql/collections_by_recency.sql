WITH recent_collections AS (
  SELECT
    IFNULL(s.SCHEDULED_CORPUS_ITEM_SCHEDULED_AT, a.REVIEWED_CORPUS_ITEM_CREATED_AT) as recency,
    s.SCHEDULED_SURFACE_IANA_TIMEZONE,
    a.*
  FROM "ANALYTICS"."DBT"."APPROVED_CORPUS_ITEMS" a
  LEFT JOIN "ANALYTICS"."DBT"."SCHEDULED_CORPUS_ITEMS" s ON (
    s.approved_corpus_item_external_id = a.approved_corpus_item_external_id
    AND s.SCHEDULED_CORPUS_ITEM_SCHEDULED_AT < current_timestamp()
    AND s.SCHEDULED_SURFACE_ID = %(SCHEDULED_SURFACE_ID)s
  )
  WHERE a.IS_COLLECTION
  AND a.CORPUS_REVIEW_STATUS = 'recommendation'
  AND a.LANGUAGE = %(LANGUAGE)s
  AND recency between DATEADD("day", %(MAX_AGE_DAYS)s, current_timestamp()) and current_timestamp()
  AND a.approved_corpus_item_external_id not in (
    'c931d2f5-0205-48f1-a773-dd0e682977b1',  -- See #incidents on 2023-03-21
    'd5edaef4-fa6c-4293-934e-33d4db207ddd' -- And again on 2024-09-19
  )

  QUALIFY row_number() OVER (PARTITION BY a.APPROVED_CORPUS_ITEM_EXTERNAL_ID ORDER BY recency DESC) = 1
)

SELECT
    approved_corpus_item_external_id as "ID",
    topic as "TOPIC",
    publisher as "PUBLISHER"
FROM recent_collections
ORDER BY recency DESC
