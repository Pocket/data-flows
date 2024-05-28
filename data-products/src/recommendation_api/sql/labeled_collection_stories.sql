with labeled_stories as (
    SELECT
        slug,
        language,
        l.value:name as label,
        JSON_EXTRACT_PATH_TEXT(s.value, 'url') as url
    FROM
        "ANALYTICS"."DBT"."COLLECTIONS",
        LATERAL FLATTEN(INPUT => labels) as l,
        lateral flatten(INPUT => stories) as s
    where language = %(LANGUAGE)s
    and label = %(COLLECTION_LABEL)s
)
select
    aci.approved_corpus_item_external_id as "ID",
    aci.topic as "TOPIC",
    aci.publisher as "PUBLISHER"
from  analytics.dbt.approved_corpus_items as aci
inner join labeled_stories as ls on ls.url = aci.url
where aci.approved_corpus_item_external_id != 'c931d2f5-0205-48f1-a773-dd0e682977b1'   -- See #incidents on 2023-03-21
and TOPIC is not null and PUBLISHER is not null -- Recommendations need to always have a topic and publisher