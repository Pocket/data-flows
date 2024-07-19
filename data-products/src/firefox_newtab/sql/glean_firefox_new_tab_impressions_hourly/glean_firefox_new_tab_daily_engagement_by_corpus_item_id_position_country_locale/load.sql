

{% import 'helpers.j2' as helpers with context %}

{% macro table_def() %}
    happened_at date not null,
    scheduled_corpus_item_id string,
    position number not null,
    source string,
    locale string,
    country string,
    impression_count number not null,
    click_count number not null,
    save_count number not null,
    dismiss_count number not null 
{% endmacro %}

{{ helpers.sf_merge(table_def(), 'happened_at') }}
