{% set sql_engine = "snowflake" %}
select max(submission_timestamp) as last_offset
from {{ destination_table_name }}