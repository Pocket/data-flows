copy into {{ destination_table_name }} (
              batch_id,
              updated_at,
              data,
              {{ metadata_keys }}
            )
        from (
            select
                {{ partition_timestamp }},
                $1:updated_at as updated_at,
                $1 as data,
                {{ metadata_values }}
            from {{ snowflake_stage_uri }}
        )
        file_format = (type = 'PARQUET')