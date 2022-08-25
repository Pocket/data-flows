from prefect import task, Flow, Parameter, context
from api_clients.pocket_snowflake_query import PocketSnowflakeQuery
import pandas as pd
import datetime
import boto3
from sagemaker.feature_store.feature_group import FeatureGroup
from sagemaker.session import Session
from prefect.schedules import IntervalSchedule

from utils import config
from utils.flow import get_flow_name

FLOW_NAME = get_flow_name(__file__)

BASE_QUERY = """
WITH prep AS (
  SELECT
    a.HASHED_USER_ID,
    a.CONTENT_ID,
    c.RESOLVED_ID,
    current_date - a.HAPPENED_AT_DAY as first_impression_age,
    MIN(a.IMPRESSION_AGE) as last_impression_age,
    SUM(a.IMPRESSION_COUNT) as total_impressions,
    MAX(a.HAPPENED_AT_DAY) as last_impressed_date
  FROM ANALYTICS.DBT.FIRST_IMPRESSED_AGE as a
  JOIN ANALYTICS.DBT.CONTENT AS c
    ON c.CONTENT_ID = a.CONTENT_ID
  WHERE CURRENT_DATE - %(AGG_WINDOW_DAYS)s <= a.TIME_ADDED  -- all imprs in the last 3 weeks
  GROUP BY 1, 2, 3, 4
  )
  
SELECT
    p.HASHED_USER_ID,
    current_date AS UPDATED_AT,
    ('[' || LISTAGG(DISTINCT p.RESOLVED_ID, ',') || ']') AS RESOLVED_IDS
FROM prep as p
WHERE ((p.first_impression_age > %(MAX_IMPR_AGE)s) OR (p.total_impressions > %(MAX_IMPRS)s))  -- filter based on time since 1st impr
GROUP BY 1, 2                                                                                 -- or total imprs
"""


@task
def transform_user_impressions_df(df: pd.DataFrame) -> pd.DataFrame:
    df["UPDATED_AT"] = df.UPDATED_AT.apply(lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ"))
    return df


@task
def load_feature_group(df: pd.DataFrame, feature_group_name):
    boto_session = boto3.Session()
    feature_store_session = Session(boto_session=boto_session,
                                    sagemaker_client=boto_session.client(service_name='sagemaker'),
                                    sagemaker_featurestore_runtime_client=boto_session.client(service_name='sagemaker-featurestore-runtime'))
    feature_group = FeatureGroup(name=feature_group_name, sagemaker_session=feature_store_session)
    feature_group.ingest(df, max_workers=4, max_processes=4, wait=True)

# Schedule to run every day
if config.ENVIRONMENT == config.ENV_PROD:
        schedule = IntervalSchedule(interval=datetime.timedelta(days=1))
else:
    schedule = None

schedule = None

with Flow(FLOW_NAME, schedule=schedule) as flow:
    feature_group = Parameter("feature group", default=f"{config.ENVIRONMENT}-user-impressions-v1")
    max_impr_age = Parameter("max impression age", default=6)
    max_impr_count = Parameter("max impression count", default=12)

    snowflake_result = PocketSnowflakeQuery()(
        query=BASE_QUERY,
        data={"MAX_IMPR_AGE": max_impr_age, "MAX_IMPRS": max_impr_count, "AGG_WINDOW_DAYS": 21}
    )

    transformed_df = transform_user_impressions_df(snowflake_result)
    load_feature_group(df=transformed_df, feature_group_name=feature_group)

if __name__ == "__main__":
    flow.run()
