-- to be run on 3XL Warehouse as a one-time run as role "loader"
    create or replace table snapshot.item.article_content_ordered_snapshot as (
    select RESOLVED_ID, 
    HTML, 
    TEXT, 
    SNOWFLAKE_LOADED_AT,
    TEXT_MD5
    from SNAPSHOT.ITEM.ARTICLE_CONTENT_V2
    order by resolved_id);