{% set sql_engine = "snowflake" %}
select max(updated_at) as last_offset
from {{ destination_table_name }}