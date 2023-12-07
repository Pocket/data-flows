{% set sql_engine = "snowflake" %}
{% set is_multi_statement = True %}
{% import 'helpers.j2' as helpers with context %}

{% macro table_def() %}
    happened_at date not null,
    feed_name string,
    users_opening_new_tab_count number not null,
    users_with_pocket_disabled_count number not null,
    users_with_spocs_disabled_count number not null,
    users_eligible_for_spocs_count number not null
{% endmacro %}

{{ helpers.sf_merge(table_def(), 'happened_at') }}