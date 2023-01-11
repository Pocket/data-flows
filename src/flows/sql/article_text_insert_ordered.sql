/*
SQL statements to insert new ordered raw data
These depend on the initial execution of the 'initial clean' statements in the sql folder
This will execute after the copy statement as a single statement
 */

set max_date = (select max(snowflake_loaded_at) from article_content_ordered_live);
        
insert into article_content_ordered_live (
    RESOLVED_ID, 
            HTML, 
            TEXT, 
            SNOWFLAKE_LOADED_AT,
            TEXT_MD5
)
select RESOLVED_ID, 
            HTML, 
            TEXT, 
            SNOWFLAKE_LOADED_AT,
            TEXT_MD5
from raw.ITEM.ARTICLE_CONTENT_V2
    where snowflake_loaded_at > $max_date
    order by resolved_id;