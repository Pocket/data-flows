{% set sql_engine = "snowflake" %}
{% set is_multi_statement = True %}
{% import 'helpers.j2' as helpers with context %}

{% macro table_def() %}
    happened_at date not null,
    feed_name string not null,
    users_viewing_recs_count number not null,
    users_clicking_recs_count number not null,
    users_eligible_for_spocs_count number not null,
    users_viewing_spocs_count number not null,
    users_clicking_spocs_count number not null,
    aggregation_date date not null
{% endmacro %}

{{ helpers.sf_merge(table_def(), 'aggregation_date') }}
