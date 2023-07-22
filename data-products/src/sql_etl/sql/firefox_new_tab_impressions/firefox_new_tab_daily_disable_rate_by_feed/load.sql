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

-- Create a select list using both the column names and data types in parquet import format
{% set select_columns_from_parquet = [] %}
{% for column, data_type in column_dict.items() %}
    {% set select_columns_from_parquet = select_columns_from_parquet + [loop.index ~ "$1:" ~ column ~ "::" ~ data_type ~ " as " ~ column] %}
{% endfor %}

-- Create a select list for the load columns that will copy into
{% set select_load_columns = [] %}
{% for column, data_type in column_dict.items() %}
    {% set select_load_columns = select_load_columns + [loop.index ~ column] %}
{% endfor %}

-- Use the select_columns list to generate the SQL query
copy into {{ destination_table_name }} 
              {{ select_load_columns | join(",\n    ") }},
              {{ metadata_keys }}
            
        from (
            select
                {{ partition_timestamp }},
                {{ select_columns_from_parquet | join(",\n    ") }},
                split(metadata$filename,'/'),
                metadata$file_row_number,
                {{ metadata_values }}
            from {{ snowflake_stage_uri }}
        )
        file_format = (type = 'PARQUET');
