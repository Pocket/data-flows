from datetime import datetime, timedelta
import prefect
from prefect import Flow, task
from prefect.schedules import IntervalSchedule
from prefect.tasks.gcp.bigquery import BigQueryTask
from utils import config
from api_clients.prefect_key_value_store_client import get_last_executed_value, update_last_executed_value

# This Flow does the following:
#   - Export Firefox engagement data from BigQuery to GCS
#   - "submission_timestamp" column is used to pull the data
#   - UTC timestamp is used for data extracts
#   - The run schedule is expected to be 5 minutes (the table gets updated in batches ~8 mins. therefore some export
#   cycles may not return data)
#

# Setting flow variables
FLOW_NAME = "FF NewTab Engagement BQ to GCS Flow"

# Export statement to export BQ data into GCS in compressed Parquet format
export_sql = """
        EXPORT DATA OPTIONS(
          uri=@gcs_uri,
          format='PARQUET',
          compression='SNAPPY',
          overwrite=true) AS
  
          SELECT *
          FROM `moz-fx-data-shared-prod.activity_stream_live.impression_stats_v1`
          where date(submission_timestamp) >= @date_partition
          and submission_timestamp > @last_executed_timestamp
    """

import_sql = """
        copy into development.gaurang_data_engineering.impression_stats_v1(
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
              s3_file_row_number
            )
        from (
            select
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
                metadata$file_row_number
            from @development.public.prefect_gcs_stage_parq/gaurang/prefect_export_test/impression_stats_v1/20220214/
        )
        file_format = (type = 'PARQUET')
        on_error=skip_file
    """


@task
def prepare_bq_export_statement(last_executed_timestamp: datetime):
    """
    Task to prepare the Export statement

    Args:
        - last_executed_timestamp: timestamp at which the export was previously executed

    Returns:
    'export_sql' the BigQuery to GCS export statement

    """
    date_partition = last_executed_timestamp.strftime('%Y-%m-%d')
    gcs_date_partition_path = last_executed_timestamp.strftime('%Y%m%d')
    batch_id = last_executed_timestamp.timestamp()

    gcs_bucket = config.GCS_BUCKET
    table_name = 'impression_stats_v1'
    gcs_path = f"{config.GCS_PATH}{table_name}"
    gcs_uri = f'gs://{gcs_bucket}/{gcs_path}/{gcs_date_partition_path}/{batch_id}_*.parq'

    logger = prefect.context.get("logger")
    logger.info(f"last_executed_timestamp: {str(last_executed_timestamp)}")
    logger.info(f"date_partition: {date_partition}")
    logger.info(f"gcs_uri: {gcs_uri}")
    logger.info(f"Export SQL:\n{export_sql}")

    return [ ('date_partition', 'STRING', date_partition),
             ('gcs_uri', 'STRING', gcs_uri),
             ('last_executed_timestamp', 'TIMESTAMP', last_executed_timestamp),
            ]

# Schedule to run every 5 minutes
schedule = IntervalSchedule(
        start_date=datetime.utcnow() + timedelta(seconds=1),
        interval=timedelta(minutes=5),
    )

with Flow(FLOW_NAME, schedule) as flow:
    last_executed_timestamp = get_last_executed_value(flow_name=FLOW_NAME,
                                                  default_if_absent=str(datetime.utcnow())
                                                  )

    export_query_param_list = prepare_bq_export_statement(
        last_executed_timestamp=last_executed_timestamp)

    bq_result = BigQueryTask()(
        query=export_sql,
        query_params=export_query_param_list,
    )

    update_last_executed_value_task = update_last_executed_value(for_flow=FLOW_NAME)

    update_last_executed_value_task.set_upstream(bq_result)

if __name__ == "__main__":
    flow.run()
