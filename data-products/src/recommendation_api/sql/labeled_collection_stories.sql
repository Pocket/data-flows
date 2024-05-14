WITH collections_with_label_expanded AS (
    SELECT
        *,
        flattened.value AS "label_value",
        label_value:name as "label"
    FROM
        "ANALYTICS"."DBT"."COLLECTIONS",
        LATERAL FLATTEN(INPUT => labels) AS "flattened"
),
labeled_collections AS (SELECT
    slug
FROM
    collections_with_label_expanded
WHERE
    label = %(COLLECTION_LABEL)s
    and language = %(LANGUAGE)s
),
collection_stories as (
    SELECT URL
    from "ANALYTICS"."DBT"."COLLECTION_STORIES"
    WHERE slug in (select * from labeled_collections)
)

SELECT
    approved_corpus_item_external_id as "ID",
    topic as "TOPIC",
    publisher as "PUBLISHER"
FROM ANALYTICS.DBT.APPROVED_CORPUS_ITEMS
WHERE url in  (select * from collection_stories)
AND approved_corpus_item_external_id <> 'c931d2f5-0205-48f1-a773-dd0e682977b1'  -- See #incidents on 2023-03-21
ORDER BY REVIEWED_CORPUS_ITEM_CREATED_AT DESC