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

    gcs_bucket = config.GCS_BUCKET
    table_name = 'impression_stats_v1'
    gcs_path = f"{config.GCS_PATH}{table_name}"
    gcs_uri = f'gs://{gcs_bucket}/{gcs_path}/{gcs_date_partition_path}/*.parq'

    logger = prefect.context.get("logger")
    logger.info(f"last_executed_timestamp: {str(last_executed_timestamp)}")
    logger.info(f"date_partition: {date_partition}")
    logger.info(f"gcs_uri: {gcs_uri}")
    logger.info(f"Export SQL:\n{export_sql}")

    return [ ('date_partition', 'STRING', date_partition),
             ('gcs_uri', 'STRING', gcs_uri),
             ('last_executed_timestamp', 'STRING', str(last_executed_timestamp)),
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
