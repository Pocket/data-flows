import datetime

from prefect import Flow
from prefect.tasks.dbt import dbt
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.executors import LocalDaskExecutor
from prefect.schedules import IntervalSchedule
from utils import config

# Downstream Flows: To run after DBT Job completes
from flows import braze_update_flow
from flows import prereview_engagement_feature_store_flow
from flows import postreview_engagement_feature_store_flow

FLOW_NAME = "DBT Orchestration Flow"
DBT_CLOUD_JOB_ID = 52822


# Schedule to run every 5 minutes
if config.ENVIRONMENT == config.ENV_PROD:
    schedule = IntervalSchedule(interval=datetime.timedelta(minutes=10))
else:
    schedule = None

with Flow(FLOW_NAME, schedule=schedule, executor=LocalDaskExecutor()) as flow:

    dbt_run_result = dbt.DbtCloudRunJob()(cause=FLOW_NAME, job_id=DBT_CLOUD_JOB_ID, wait_for_job_run_completion=True)

    # TODO: To start updating Braze based on Snowplow:
    # 1. In the KV-store set production/braze_update_flow/last_loaded_at to the etl_tstamp of the first Snowplow event
    #    we should process.
    # 2. Enable the code below and merge it into main.
    # braze_flow_id = create_flow_run(flow_name=braze_update_flow.FLOW_NAME, project_name=config.PREFECT_PROJECT_NAME)
    # braze_wait_task = wait_for_flow_run(braze_flow_id, raise_final_state=True)
    # braze_flow_id.set_upstream(dbt_run_result)

    prereview_flow_id = create_flow_run(flow_name=prereview_engagement_feature_store_flow.FLOW_NAME, project_name=config.PREFECT_PROJECT_NAME)
    prereview_wait_task = wait_for_flow_run(prereview_flow_id, raise_final_state=True)
    prereview_flow_id.set_upstream(dbt_run_result)

    postreview_flow_id = create_flow_run(flow_name=postreview_engagement_feature_store_flow.FLOW_NAME, project_name=config.PREFECT_PROJECT_NAME)
    postreview_wait_task = wait_for_flow_run(postreview_flow_id, raise_final_state=True)
    postreview_flow_id.set_upstream(dbt_run_result)

# for execution in development only
if __name__ == "__main__":
    # flow.visualize()
    flow.run()
