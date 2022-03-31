from prefect import Flow
from prefect.tasks.aws.s3 import S3Download

from utils.flow import get_flow_name

FLOW_NAME = get_flow_name(__file__)

s3_bucket_source = 'pocket-data-learning-dev'
s3_bucket_key='analytics-modeled-data/parquet/dbt/web_explore_impressions_clicks_by_item_hour/data_0_0_0.snappy.parquet'

with Flow(FLOW_NAME) as flow:
    s3download_result = S3Download()(key=s3_bucket_key, bucket=s3_bucket_source, as_bytes=True)

if __name__ == "__main__":
    flow.run()
