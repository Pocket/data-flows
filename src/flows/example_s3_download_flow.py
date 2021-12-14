from os import environ

import prefect
from prefect import task, Flow
from prefect.run_configs import ECSRun
from prefect.tasks.aws.s3 import S3Download


s3_bucket_source = 'pocket-data-learning-dev'
s3_bucket_key='analytics-modeled-data/parquet/dbt/web_explore_impressions_clicks_by_item_hour/data_0_0_0.snappy.parquet'

with Flow("s3download_flow") as flow:
    s3download_function = S3Download()
    s3download_resut = s3download_function(key=s3_bucket_key, bucket=s3_bucket_source)

# TODO: In production, the steps below would be taken by a deployment script. They're just included here as an example.
flow.storage = prefect.storage.S3(
    bucket='pocket-dataflows-storage-dev',
    add_default_labels=False
)

flow.run_config = ECSRun(
    # task_definition_path="test.yaml",
    labels=['Dev'],
    task_role_arn=environ.get('PREFECT_TASK_ROLE_ARN'),
    # execution_role_arn='arn:aws:iam::12345678:role/prefect-ecs',
    image='prefecthq/prefect:latest-python3.9',
)

flow.register(project_name="prefect-tutorial")
