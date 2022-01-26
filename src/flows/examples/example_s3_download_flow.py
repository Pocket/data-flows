from prefect import Flow
from prefect.tasks.aws.s3 import S3Download

s3_bucket_source = 'pocket-data-learning-dev'
s3_bucket_key='analytics-modeled-data/parquet/dbt/web_explore_impressions_clicks_by_item_hour/data_0_0_0.snappy.parquet'

s3_download = S3Download()

with Flow("s3download_flow") as flow:
    s3download_result = s3_download(key=s3_bucket_key, bucket=s3_bucket_source, as_bytes=True)

if __name__ == "__main__":
    flow.run()
