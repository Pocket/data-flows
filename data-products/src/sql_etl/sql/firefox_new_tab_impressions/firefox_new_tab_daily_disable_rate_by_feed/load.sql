{% set sql_engine = "snowflake" %}

-- Define a list of columns and data types as a dictionary
{% set column_dict = {
    "happened_at": "date",
    "feed_name": "string",
    "users_opening_new_tab_count": "integer",
    "users_with_pocket_disabled_count": "integer",
    "users_with_spocs_disabled_count": "integer",
    "users_eligible_for_spocs_count": "integer"
} %}

-- Create a select list using only the column names from column dict
{% set merge_columns = [] %}
{% for column, data_type in column_dict.items() %}
  {% do merge_columns.append(column) %}
{% endfor %}

-- Create a select list using both the column names and data types in parquet import format
{% set select_columns = [] %}
{% for column, data_type in column_dict.items() %}
    {% set select_column = loop.index ~ "$1:" ~ column ~ "::" ~ data_type ~ " as " ~ column %}
    {% do select_columns.append(select_column) %}
{% endfor %}

merge into {{ destination_table_name }} (
              {{ merge_columns | join(",\n    ") }},
              {{ metadata_keys }}
            )
        from (
            select
                {{ partition_timestamp }},
                {{ select_columns | join(",\n    ") }},
                split(metadata$filename,'/'),
                metadata$file_row_number,
                {{ metadata_values }}
            from {{ snowflake_stage_uri }}
        )
        file_format = (type = 'PARQUET')