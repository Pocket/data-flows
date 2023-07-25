{% set sql_engine = "snowflake" %}
{% set is_multi_statement = True %}

{% macro table_columns() %}
    happened_at date not null,
    tile_id number not null,
    position number not null,
    impression_count number not null,
    click_count number not null,
    save_count number not null,
    dismiss_count number not null 
{% endmacro %}

create temporary table {{ table_name }}_tmp (
    {{ table_columns() }},
    {{ metadata_column_definitions }}
);

copy into {{ table_name }}_tmp 
from (select
        $1:happened_at,
        $1:tile_id,
        $1:position,
        $1:impression_count,
        $1:click_count,
        $1:save_count,
        $1:dismiss_count,
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