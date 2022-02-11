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
#   TODOs:
#   - Handling edge conditions during the turn of the day when the data partition switches to the next day
#

# Setting flow variables
FLOW_NAME = "FF NewTab Engagement BQ to GCS Flow"

@task
def prepare_bq_export_statement(last_executed_timestamp: datetime) -> str:
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


    extract_sql = f"""
            SELECT *
            FROM `moz-fx-data-shared-prod.activity_stream_live.impression_stats_v1`
            where date(submission_timestamp) = '{date_partition}'
            and submission_timestamp > '{str(last_executed_timestamp)}'
        """

    gcs_uri = f'gs://{gcs_bucket}/{gcs_path}/{gcs_date_partition_path}/*.parq'
    # Export statement to export BQ data into GCS in compressed Parquet format
    export_sql = f"""
            EXPORT DATA OPTIONS(
              uri='{gcs_uri}',
              format='PARQUET',
              compression='SNAPPY',
              overwrite=true) AS
            {extract_sql}
        """
    logger = prefect.context.get("logger")
    logger.info(f"Export SQL:\n{export_sql}")
    return export_sql

# Schedule to run every 5 minutes
schedule = IntervalSchedule(
        start_date=datetime.utcnow() + timedelta(seconds=1),
        interval=timedelta(minutes=5),
    )

with Flow(FLOW_NAME, schedule) as flow:
    last_executed_timestamp = get_last_executed_value(flow_name=FLOW_NAME,
                                                      default_if_absent=str(datetime.utcnow())
                                                      )
    export_sql = prepare_bq_export_statement(last_executed_timestamp=last_executed_timestamp)

    bq_result = BigQueryTask()(query=export_sql)

    update_last_executed_value_task = update_last_executed_value(for_flow=FLOW_NAME, store_as_utc=True)

    promised_update_last_executed_flow_result = update_last_executed_value_task.set_upstream(bq_result)

if __name__ == "__main__":
    flow.run()
