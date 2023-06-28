SELECT
{% if for_new_offset %}
    max(submission_timestamp) as new_offset
{% else %}
    *   
{% endif %}
FROM 
{% if for_backfill %}
`moz-fx-data-shared-prod.activity_stream_stable.impression_stats_v1`
{% else %}
`moz-fx-data-shared-prod.activity_stream_live.impression_stats_v1`  
{% endif %}
where submission_timestamp >= '{{ batch_start }}'
and submission_timestamp < '{{ batch_end }}'
and page != 'https://newtab.firefoxchina.cn/newtab/as/activity-stream.html'  -- exclude non-Pocket CN NewTab
and loaded is null --don't include loaded ping
and array_length(tiles) >= 1 --making sure data is valid/non-empty
{% if for_new_offset %}
{% else %}
qualify row_number() over (partition by document_id order by submission_timestamp desc) = 1
{% endif %}
