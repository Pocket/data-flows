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

# List of flow names that will be run after the Dbt job run has finished successfully.
DBT_DOWNSTREAM_FLOW_NAMES = [
    # braze_update_flow.FLOW_NAME,  # Enable this and set production/braze_update_flow/last_loaded_at on April 13th.
    prereview_engagement_feature_store_flow.FLOW_NAME,
    postreview_engagement_feature_store_flow.FLOW_NAME,
]

# Schedule to run every 5 minutes
if config.ENVIRONMENT == config.ENV_PROD:
    schedule = IntervalSchedule(interval=datetime.timedelta(minutes=10))
else:
    schedule = None

with Flow(FLOW_NAME, schedule=schedule, executor=LocalDaskExecutor()) as flow:

    dbt_run_result = dbt.DbtCloudRunJob()(cause=FLOW_NAME, job_id=DBT_CLOUD_JOB_ID, wait_for_job_run_completion=True)

    # Create and wait for all flows that should be run downstream of the above Dbt job run.
    for downstream_flow_name in DBT_DOWNSTREAM_FLOW_NAMES:
        flow_id = create_flow_run(
            flow_name=downstream_flow_name,
            project_name=config.PREFECT_PROJECT_NAME,
            task_args=dict(name=f"create_flow_run({downstream_flow_name})"),
            upstream_tasks=[dbt_run_result],
        )

        wait_for_flow_run(
            flow_id,
            raise_final_state=True,
            task_args=dict(name=f"create_flow_run({downstream_flow_name})"),
        )

# for execution in development only
if __name__ == "__main__":
    # flow.visualize()
    flow.run()
