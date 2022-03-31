from prefect import task, Flow, Parameter, unmapped
from prefect.tasks.aws.s3 import S3List
import pandas as pd
import boto3
from sagemaker.feature_store.feature_group import FeatureGroup
from sagemaker.session import Session

from utils.flow import get_flow_name

FLOW_NAME = get_flow_name(__file__)


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
    boto_session = boto3.Session()
    feature_store_session = Session(boto_session=boto_session,
                                    sagemaker_client=boto_session.client(service_name='sagemaker'),
                                    sagemaker_featurestore_runtime_client=boto_session.client(service_name='sagemaker-featurestore-runtime'))
    feature_group = FeatureGroup(name=feature_group_name, sagemaker_session=feature_store_session)
    feature_group.ingest(df, max_workers=4, max_processes=4, wait=True)


with Flow(FLOW_NAME) as flow:
    feature_group = Parameter("feature group", default="user-impressions")
    snowflake_bucket = "pocket-data-learning"
    snowflake_prefix = "analytics-modeled-data/parquet/user_impressions"
    list_files_task = S3List()
    parquet_files = list_files_task(bucket=snowflake_bucket, prefix=snowflake_prefix)
    dfs = extract_parquet_as_df.map(parquet_file=parquet_files, bucket=unmapped(snowflake_bucket))
    xdfs = transform_user_impressions_df.map(dfs)
    load_featue_group.map(df=xdfs, feature_group_name=unmapped(feature_group))

if __name__ == "__main__":
    flow.run()
