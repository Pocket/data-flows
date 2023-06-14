copy into {table_name} (
              batch_id,
              submission_timestamp,
              additional_properties,
              addon_version,
              block,
              click,
              document_id,
              impression_id,
              loaded,
              locale,
              metadata,
              normalized_app_name,
              normalized_channel,
              normalized_country_code,
              normalized_os,
              normalized_os_version,
              page,
              pocket,
              profile_creation_date,
              region,
              release_channel,
              sample_id,
              shield_id,
              source,
              tiles,
              user_prefs,
              version,
              experiments,
              s3_filename_splitpath, 
              s3_file_row_number,
              {metadata_keys}
            )
        from (
            select
                {batch_id},
                to_timestamp($1:submission_timestamp::integer/1000000) as submission_timestamp,
                $1:additional_properties::string as additional_properties,
                $1:addon_version::string as addon_version,
                $1:block::integer as block,
                $1:click::integer as click,
                $1:document_id::string as document_id,
                $1:impression_id::string as impression_id,
                $1:loaded::integer as loaded,
                $1:locale::string as locale,
                $1:metadata as metadata,
                $1:normalized_app_name::string as normalized_app_name,
                $1:normalized_channel::string as normalized_channel,
                $1:normalized_country_code::string as normalized_country_code,
                $1:normalized_os::string as normalized_os,
                $1:normalized_os_version::string as normalized_os_version,
                $1:page::string as page,
                $1:pocket::integer as pocket,
                $1:profile_creation_date::integer as profile_creation_date,
                $1:region::string as region,
                $1:release_channel::string as release_channel,
                $1:sample_id::integer as sample_id,
                $1:shield_id::string as shield_id,
                $1:source::string as source,
                $1:tiles as tiles,
                $1:user_prefs::integer as user_prefs,
                $1:version::string as version,
                $1:experiments as experiments,
                split(metadata$filename,'/'),
                metadata$file_row_number,
                {metadata_values}
            from {snowflake_stage_uri}
        )
        file_format = (type = 'PARQUET')