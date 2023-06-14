SELECT *
FROM `moz-fx-data-shared-prod.activity_stream_live.impression_stats_v1`
where submission_timestamp > '{current_offset}'
and page != 'https://newtab.firefoxchina.cn/newtab/as/activity-stream.html'  -- exclude non-Pocket CN NewTab
and loaded is null --don't include loaded ping
and array_length(tiles) >= 1 --making sure data is valid/non-empty
qualify row_number() over (partition by document_id order by submission_timestamp desc) = 1