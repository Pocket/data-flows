from os import environ

import prefect
from prefect import task, Flow, Parameter, unmapped
from prefect.run_configs import ECSRun
from prefect.tasks.aws.s3 import S3List
import pandas as pd
import boto3


@task
def extract_parquet_as_df(bucket:str, parquet_file: str):
    parquet_file = f"s3://{bucket}/{parquet_file}"
    return pd.read_parquet(parquet_file)


@task
def transform_user_impressions_df(df: pd.DataFrame):
    df = df.rename(columns={"USER_ID": "user_id",
                            "RESOLVED_IDS": "resolved_ids",
                            "LAST_UPDATED_AT": "updated_at"}).astype({"user_id": int})
    df["updated_at"] = df.updated_at.apply(lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ"))
    return df

@task
def load_featue_group(df: pd.DataFrame, feature_group_name):
    fs_client = boto3.client("sagemaker-featurestore-runtime")
    for row in df.itertuples(index=False):
        record = [{"FeatureName": k, "ValueAsString": str(getattr(row, k))} for k in row._fields]

        fs_client.put_record(
            FeatureGroupName=feature_group_name,
            Record=record)


with Flow("User Impression Feature Group Flow") as flow:
    feature_group = Parameter("feature group", default="user-impressions")
    snowflake_bucket = "pocket-data-learning"
    snowflake_prefix = "analytics-modeled-data/parquet/user_impressions"
    list_files_task = S3List()
    parquet_files = list_files_task(bucket=snowflake_bucket, prefix=snowflake_prefix)
    dfs = extract_parquet_as_df.map(parquet_file=parquet_files, bucket=unmapped(snowflake_bucket))
    xdfs = transform_user_impressions_df.map(dfs)
    load_featue_group.map(df=xdfs, feature_group_name=unmapped(feature_group))

# flow.run()

flow.storage = prefect.storage.S3(
    bucket='pocket-dataflows-storage-prod',
    add_default_labels=False
)

flow.run_config = ECSRun(
    labels=['Prod'],
    image='996905175585.dkr.ecr.us-east-1.amazonaws.com/dataflows-prod-app:latest',
)

flow.register(project_name="prefect-tutorial")
