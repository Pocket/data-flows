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
      r.HASHED_USER_ID,
      r.CORPUS_ITEM_ID,
      COUNT(*) as impression_count,
      MIN(i.happened_at) as first_impression_time
  FROM "ANALYTICS"."DBT"."IMPRESSIONS" i
  JOIN "ANALYTICS"."DBT_STAGING"."STG_CORPUS_SLATE_RECOMMENDATIONS" r on i.CORPUS_RECOMMENDATION_ID = r.CORPUS_RECOMMENDATION_ID
  WHERE i.HAPPENED_AT > CURRENT_DATE - %(AGG_WINDOW_DAYS)s
    AND r.HASHED_USER_ID IS NOT NULL
  GROUP BY 1,2
  ORDER BY impression_count DESC
)

SELECT 
    "HASHED_USER_ID",
    current_date as "UPDATED_AT",
    ('[' || LISTAGG(DISTINCT CORPUS_ITEM_ID, ',') || ']') AS "CORPUS_IDS"
FROM 
    (SELECT * FROM prep WHERE impression_count > %(MAX_IMPRS)s
     UNION 
     SELECT * FROM prep WHERE first_impression_time < CURRENT_DATE - %(MAX_IMPR_AGE)s )
GROUP BY 1,2
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

with Flow(FLOW_NAME, schedule=schedule) as flow:
    feature_group = Parameter("feature group", default=f"{config.ENVIRONMENT}-user-impressions-v2")
    max_impr_age = Parameter("max impression age", default=14)
    max_impr_count = Parameter("max impression count", default=9)

    snowflake_result = PocketSnowflakeQuery()(
        query=BASE_QUERY,
        data={"MAX_IMPR_AGE": max_impr_age, "MAX_IMPRS": max_impr_count, "AGG_WINDOW_DAYS": 29}
    )

    transformed_df = transform_user_impressions_df(snowflake_result)
    load_feature_group(df=transformed_df, feature_group_name=feature_group)

if __name__ == "__main__":
    flow.run()
