{% set sql_engine = "bigquery" %}
SELECT
{% if for_new_offset %}
    max(updated_at) as new_offset
{% else %}
    *   
{% endif %}
FROM 
{% if for_backfill %}
from {{ source_table_name }}_stable
{% else %}
from {{ source_table_name }}
{% endif %}
where updated_at >= '{{ batch_start }}'
and updated_at < '{{ batch_end }}'
