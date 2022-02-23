from prefect import Flow
from prefect.tasks.gcp.bigquery import BigQueryTask

from utils import config

FLOW_NAME = "example BQ to GCS Export"

# GCS Bucket and Path to export BigQuery data
gcs_bucket = config.GCS_BUCKET
table_name = 'impression_stats_v1'
gcs_path = f"{config.GCS_PATH}/{table_name}"

# BigQuery SQL to extract data
#   date_partition : BigQuery table partition to extract (Note: the BQ table is partitioned by the
#                   "submission_timestamp" field
date_partition = '20220207'
extract_sql = f"""
        SELECT *
        FROM `moz-fx-data-shared-prod.activity_stream_live.impression_stats_v1`
        where date(submission_timestamp) = PARSE_DATE("%Y%m%d", @date_partition)
    """

# Export statement to export BQ data into GCS in compressed Parquet format
export_sql = f"""
        EXPORT DATA OPTIONS(
          uri=@gcs_uri,
          format='PARQUET',
          compression='SNAPPY',
          overwrite=true) AS
        {extract_sql}
    """

with Flow(FLOW_NAME) as flow:
    # BigQueryTask: (https://docs.prefect.io/api/latest/tasks/gcp.html#bigquerytask)
    bq_result = BigQueryTask()(
        query=export_sql,
        query_params=[('date_partition', 'STRING', date_partition),
                      ('gcs_uri', 'STRING', f'gs://{gcs_bucket}/{gcs_path}/{date_partition}/*.parq')],
    )

if __name__ == "__main__":
    flow.run()
