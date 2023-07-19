{% set sql_engine = "bigquery" %}
{% if for_new_offset %}
    select current_timestamp()
{% else %}
{% macro parse_iso8601(datetime) %}
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', '{{ datetime }}')
{% endmacro %}
-- sql here
{% endif %}