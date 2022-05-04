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
WITH 
prep as (
  SELECT
    a.HASHED_USER_ID,
    a.CONTENT_ID,
    c.RESOLVED_ID,
    MAX(a.IMPRESSION_AGE) as max_impression_age,
    SUM(a.IMPRESSION_COUNT) as total_imprs,
    MAX(a.HAPPENED_AT_DAY) as HAPPENED_AT
  FROM ANALYTICS.DBT.FIRST_IMPRESSED_AGE as a
  JOIN ANALYTICS.DBT.CONTENT AS c
    ON c.CONTENT_ID = a.CONTENT_ID
  -- WHERE SLATE_LINEUP_ID in ('05027beb-0053-4020-8bdc-4da2fcc0cb68', '76b3f758-6aa3-430b-bcf1-8867897692b2')  -- wh persv2 lineups
  --  AND CURRENT_DATE - HAPPENED_AT_DAY < 15  -- two weeks
  WHERE CURRENT_DATE - DATE(a.TIME_ADDED) < 15  -- two weeks 
  GROUP BY 1, 2, 3
  )
  
SELECT
    u.USER_ID,
    p.HAPPENED_AT AS UPDATED_AT,
    LISTAGG(DISTINCT p.RESOLVED_ID, ',') AS RESOLVED_IDS
FROM prep as p
JOIN ANALYTICS.DBT.USERS as u
  ON p.HASHED_USER_ID = u.HASHED_USER_ID
WHERE (p.max_impression_age > %(MAX_AGE)s) OR (p.total_imprs > %(MAX_IMPRS)s)
GROUP BY 1, 2
"""


@task
def transform_user_impressions_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"USER_ID": "user_id",
                            "RESOLVED_IDS": "resolved_ids",
                            "UPDATED_AT": "updated_at"}).astype({"user_id": int})
    df["updated_at"] = df.updated_at.apply(lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ"))
    return df


@task
def load_feature_group(df: pd.DataFrame, feature_group_name):
    boto_session = boto3.Session()
    feature_store_session = Session(boto_session=boto_session,
                                    sagemaker_client=boto_session.client(service_name='sagemaker'),
                                    sagemaker_featurestore_runtime_client=boto_session.client(service_name='sagemaker-featurestore-runtime'))
    feature_group = FeatureGroup(name=feature_group_name, sagemaker_session=feature_store_session)
    feature_group.ingest(df, max_workers=4, max_processes=4, wait=True)

# Schedule to run every 5 minutes
if config.ENVIRONMENT == config.ENV_PROD:
    schedule = IntervalSchedule(interval=datetime.timedelta(hours=1))
else:
    schedule = None

with Flow(FLOW_NAME, schedule=schedule) as flow:
    logger = context.get("logger")
    feature_group = Parameter("feature group", default="user-impressions")
    max_impr_age = Parameter("max impression age", default=6)
    max_impr_count = Parameter("max impression count", default=12)

    snowflake_result = PocketSnowflakeQuery()(
        query=BASE_QUERY,
        data={"MAX_AGE": max_impr_age, "MAX_IMPRS": max_impr_count}
    )

    xdf = transform_user_impressions_df(snowflake_result)
    load_feature_group(df=xdf, feature_group_name=feature_group)

if __name__ == "__main__":
    flow.run()
