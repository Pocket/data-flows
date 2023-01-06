-- to be run on 3XL Warehouse as a one-time run as role "loader"
    create or replace table snapshot.item.article_content_ordered_snapshot as (
    select RESOLVED_ID, 
    HTML, 
    TEXT, 
    SNOWFLAKE_LOADED_AT,
    TEXT_MD5
    from SNAPSHOT.ITEM.ARTICLE_CONTENT_V2
    order by resolved_id);

-- grants needed on new table
    grant ownership on table snapshot.item.article_content_ordered_snapshot to role LOADER REVOKE CURRENT GRANTS;
    grant select, delete on table snapshot.item.article_content_ordered_snapshot to role USER_DATA_DELETION_ROLE;
    grant all on table snapshot.item.article_content_ordered_snapshot to role ML_SERVICE_ROLE;
    grant select on table snapshot.item.article_content_ordered_snapshot to role TRANSFORMER;
    grant select on table snapshot.item.article_content_ordered_snapshot to role SELECT_ALL_ROLE;