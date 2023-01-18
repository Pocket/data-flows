/*
This will run after an hourly update only once on the weekend
 */

use warehouse dpt_wh_3xl;

create or replace table article_content_ordered_new as (
select RESOLVED_ID, 
HTML, 
TEXT, 
SNOWFLAKE_LOADED_AT,
TEXT_MD5,
current_timestamp() as last_ordered
from article_content_ordered_live
qualify row_number() over (partition by resolved_id order by snowflake_loaded_at desc) = 1
order by resolved_id);

-- apply proper grants to new ordered table before swap

grant select, delete on table article_content_ordered_new to role USER_DATA_DELETION_ROLE;
grant select on table article_content_ordered_new to role TRANSFORMER;
grant select on table article_content_ordered_new to role SELECT_ALL_ROLE;

-- swap and drop old table

alter table article_content_ordered_live swap with article_content_ordered_new;
drop table article_content_ordered_new;