import datetime

from prefect import Flow
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.executors import LocalDaskExecutor
from prefect.schedules import IntervalSchedule
from utils import config
from utils.config import PREFECT_PROJECT_NAME

# DBT Job Flow
from flows import dbt_job_flow

# Downstream Flows: To run after DBT Job completes
from flows import braze_update_flow
from flows import prereview_engagement_feature_store_flow
from flows import postreview_engagement_feature_store_flow

FLOW_NAME = "DBT Orchestration Flow"


# Schedule to run every 5 minutes
if config.ENVIRONMENT == config.ENV_PROD:
    schedule = IntervalSchedule(interval=datetime.timedelta(minutes=10))
else:
    schedule = None

with Flow(FLOW_NAME, schedule=schedule, executor=LocalDaskExecutor()) as flow:

    dbt_job_flow_id = create_flow_run(
        flow_name=dbt_job_flow.FLOW_NAME,
        project_name=PREFECT_PROJECT_NAME,
        task_run_name=f"create {dbt_job_flow.FLOW_NAME}",
    )
    dbt_job_wait_task = wait_for_flow_run(
        dbt_job_flow_id,
        raise_final_state=True,
        task_run_name=f"wait for {dbt_job_flow.FLOW_NAME}"
    )

    braze_flow_id = create_flow_run(
        flow_name=braze_update_flow.FLOW_NAME,
        project_name=config.PREFECT_PROJECT_NAME,
        task_run_name=f"create {braze_update_flow.FLOW_NAME}",
    )
    braze_wait_task = wait_for_flow_run(
        braze_flow_id,
        raise_final_state=True,
        task_run_name=f"wait for {braze_update_flow.FLOW_NAME}",
    )
    braze_flow_id.set_upstream(dbt_job_wait_task)

    prereview_flow_id = create_flow_run(
        flow_name=prereview_engagement_feature_store_flow.FLOW_NAME,
        project_name=config.PREFECT_PROJECT_NAME,
        task_run_name=f"create {prereview_engagement_feature_store_flow.FLOW_NAME}",
    )
    prereview_wait_task = wait_for_flow_run(
        prereview_flow_id,
        raise_final_state=True,
        task_run_name=f"wait for {prereview_engagement_feature_store_flow.FLOW_NAME}",
    )
    prereview_flow_id.set_upstream(dbt_job_wait_task)

    postreview_flow_id = create_flow_run(
        flow_name=postreview_engagement_feature_store_flow.FLOW_NAME,
        project_name=config.PREFECT_PROJECT_NAME,
        task_run_name=f"create {postreview_engagement_feature_store_flow.FLOW_NAME}",
    )
    postreview_wait_task = wait_for_flow_run(
        postreview_flow_id,
        raise_final_state=True,
        task_run_name=f"wait for {postreview_engagement_feature_store_flow.FLOW_NAME}",
    )
    postreview_flow_id.set_upstream(dbt_job_wait_task)

# for execution in development only
if __name__ == "__main__":
    # flow.visualize()
    flow.run()
