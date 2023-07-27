{% set sql_engine = "snowflake" %}
{% set is_multi_statement = True %}
{% import 'helpers.j2' as helpers with context %}

{% macro table_def() %}
    happened_at date not null,
    feed_name string,
    position number not null,
    new_tab_impression_count number not null,
    position_total_impression_count number not null,
    position_spoc_impression_count number not null
{% endmacro %}

{{ helpers.sf_merge(table_def(), 'happened_at') }}
