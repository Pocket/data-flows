-- to be run on 3XL Warehouse as a one-time run as role "loader"
    create or replace table raw.item.article_content_ordered_live as (
    select RESOLVED_ID, 
    HTML, 
    TEXT, 
    SNOWFLAKE_LOADED_AT,
    TEXT_MD5,
    current_timestamp() as last_ordered
    from RAW.ITEM.ARTICLE_CONTENT_V2
    qualify row_number() over (partition by resolved_id order by snowflake_loaded_at desc) = 1
    order by resolved_id);

-- grants needed on new table
    grant ownership on table raw.item.article_content_ordered_live to role LOADER REVOKE CURRENT GRANTS;
    grant select, delete on table raw.item.article_content_ordered_live to role USER_DATA_DELETION_ROLE;
    grant all on table raw.item.article_content_ordered_live to role ML_SERVICE_ROLE;
    grant select on table raw.item.article_content_ordered_live to role TRANSFORMER;
    grant select on table raw.item.article_content_ordered_live to role SELECT_ALL_ROLE;