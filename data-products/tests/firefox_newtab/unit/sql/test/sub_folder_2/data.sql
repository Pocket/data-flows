SELECT
    *   
FROM 
{% if for_backfill %}
from {{ source_table_name }}_stable
{% else %}
from {{ source_table_name }}
{% endif %}
where updated_at >= '{{ batch_start }}'
and updated_at < '{{ batch_end }}'
