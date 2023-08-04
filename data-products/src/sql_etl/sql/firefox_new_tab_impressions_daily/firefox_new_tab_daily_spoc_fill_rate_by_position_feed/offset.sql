{% set sql_engine = "snowflake" %}
select (trunc(sysdate()::timestamp, 'day') - interval '1 day') - interval '1 microsecond';