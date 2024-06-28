{% set sql_engine = "snowflake" %}
{% set is_multi_statement = True %}
{% import 'helpers.j2' as helpers with context %}

{% macro table_def() %}
    happened_at date not null,
    recommendation_id string, 
    tile_id number,
    position number not null,
    impression_count number not null,
    click_count number not null,
    save_count number not null,
    dismiss_count number not null 
{% endmacro %}

{{ helpers.sf_merge(table_def(), 'happened_at') }}
