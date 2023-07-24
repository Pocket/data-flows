{% set sql_engine = "snowflake" %}
{% set is_multi_statement = True %}

{% macro table_columns() %}
    happened_at date not null,
    feed_name  string,
    users_opening_new_tab_count number not null,
    users_with_pocket_disabled_count number not null,
    users_with_spocs_disabled_count number not null,
    users_eligible_for_spocs_count number not null
{% endmacro %}

create temporary table {{ table_name }}_tmp (
    {{ table_columns() }},
    {{ metadata_column_definitions }}
);

copy into {{ table_name }}_tmp 
from (select
        $1:happened_at,
        $1:feed_name,
        $1:users_opening_new_tab_count,
        $1:users_with_pocket_disabled_count,
        $1:users_with_spocs_disabled_count,
        $1:users_eligible_for_spocs_count,
        {{ metadata_values }}
    from {{ snowflake_stage_uri }}
);

create table if not exists {{ table_name }}  like {{ table_name }}_tmp;

begin;
delete from {{ table_name }} 
where happened_at = '{{ batch_start }}'::date;
insert into {{ table_name }} 
select * from {{ table_name }}_tmp;
commit;