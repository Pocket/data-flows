from prefect import Flow
from prefect.tasks.gcp.bigquery import BigQueryTask

# GCS Bucket and Path to export BigQuery data
gcs_bucket = 'pocket-airflow-nonprod-stage'
gcs_path = 'gaurang/prefect_export_test/impression_stats_v1'

# BigQuery SQL to extract data
#   date_partition : BigQuery table partition to extract (Note: the BQ table is partitioned by the
#                   "submission_timestamp" field
date_partition = '2022-02-08'
extract_sql = f"""
        SELECT *
        FROM `moz-fx-data-shared-prod.activity_stream_live.impression_stats_v1`
        where date(submission_timestamp) = @date_partition
    """

# Export statement to export BQ data into GCS in compressed Parquet format
export_sql = f"""
        EXPORT DATA OPTIONS(
          uri='gs://{gcs_bucket}/{gcs_path}/{date_partition}/*.parq',
          format='PARQUET',
          compression='SNAPPY',
          overwrite=true) AS
        {extract_sql}
    """

with Flow("example Snowflake query") as flow:
    # BigQueryTask: (https://docs.prefect.io/api/latest/tasks/gcp.html#bigquerytask)
    bq_result = BigQueryTask()(
        query=export_sql,
        query_params=[('date_partition', 'STRING', date_partition),],
    )

if __name__ == "__main__":
    flow.run()
