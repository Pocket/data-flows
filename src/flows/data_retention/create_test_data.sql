-- This script created test data table in the previously created DB: <test_db> and Schema: <test_schema> used for
-- testing purposes

-- example: set raw_schema = 'development.gaurang_data_deletion_rawdata';
set raw_schema = '<test_db>.<test_schema>';
use schema identifier($raw_schema);


set (start_date, end_date) = ('2022-06-01', '2022-06-15');

-- Snowplow events
create transient table EVENTS as
select e.*
FROM SNOWPLOW.ATOMIC.EVENTS as e
where derived_tstamp between $start_date and $end_date
;

-- Firehose Raw data
create transient table AB_TEST_TRACK as
select *
from raw.firehose.AB_TEST_TRACK as s
where s.snowflake_loaded_at between $start_date and $end_date
;

create transient table ITEM_SESSION as
select *
from raw.firehose.ITEM_SESSION as s
where s.snowflake_loaded_at between $start_date and $end_date
;

create transient table PINPOINT as
select *
from raw.firehose.PINPOINT as s
where s.snowflake_loaded_at between $start_date and $end_date
;

create transient table POCKET_V3_SEND as
select *
from raw.firehose.POCKET_V3_SEND as s
where s.snowflake_loaded_at between $start_date and $end_date
;

create transient table PREMIUM_SUBSCRIPTION_CREATED as
select *
from raw.firehose.PREMIUM_SUBSCRIPTION_CREATED as s
where s.snowflake_loaded_at between $start_date and $end_date
;

create transient table PREMIUM_SUBSCRIPTION_ENDED as
select *
from raw.firehose.PREMIUM_SUBSCRIPTION_ENDED as s
where s.snowflake_loaded_at between $start_date and $end_date
;

create transient table PREMIUM_SUBSCRIPTION_RENEWED as
select *
from raw.firehose.PREMIUM_SUBSCRIPTION_RENEWED as s
where s.snowflake_loaded_at between $start_date and $end_date
;

create transient table SENDGRID_EVENTS as
select *
from raw.firehose.SENDGRID_EVENTS as s
where s.snowflake_loaded_at between $start_date and $end_date
;

-- TODO: UNIFIED_EVENT_ALL (If this is not used, can we drop all data and stop the Snowpipe from loading new data?)

create transient table USER_ACTION as
select *
from raw.firehose.USER_ACTION as s
where s.snowflake_loaded_at between $start_date and $end_date
;

create transient table USER_ITEM_TAGS_ADDED as
select *
from raw.firehose.USER_ITEM_TAGS_ADDED as s
where s.snowflake_loaded_at between $start_date and $end_date
;

create transient table USER_LIST_ITEM_CREATED as
select *
from raw.firehose.USER_LIST_ITEM_CREATED as s
where s.snowflake_loaded_at between $start_date and $end_date
;

create transient table USER_TRACK_ADV as
select *
from raw.firehose.USER_TRACK_ADV as s
where s.snowflake_loaded_at between $start_date and $end_date
;

create transient table WEB_TRACK as
select *
from raw.firehose.WEB_TRACK as s
where s.snowflake_loaded_at between $start_date and $end_date
;
