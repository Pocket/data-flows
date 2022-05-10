from prefect import Flow
from prefect.schedules.clocks import CronClock
from prefect.tasks.dbt import dbt
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.executors import LocalDaskExecutor
from prefect.schedules import Schedule
from utils import config

from flows.prospecting import prereview_engagement_feature_store_flow, postreview_engagement_feature_store_flow, prospect_flow
from utils.flow import get_flow_name

FLOW_NAME = get_flow_name(__file__)
DBT_CLOUD_JOB_ID = 52822

# List of flow names that will be run after the Dbt job run has finished successfully.
DATA_FLOWS = [
    prereview_engagement_feature_store_flow.FLOW_NAME,
    postreview_engagement_feature_store_flow.FLOW_NAME,
    prospect_flow
]

if config.ENVIRONMENT == config.ENV_PROD:
    schedule = Schedule(clocks=[CronClock("0 0 * * *")]) # Nightly at midnight UTC
else:
    schedule = None

@task
def execute_flow(flow_name, upstream_tasks):
    flow_id = create_flow_run(
        flow_name=flow_name,
        project_name=config.PREFECT_PROJECT_NAME,
        task_args=dict(name=f"create_flow_run({flow_name})"),
        upstream_tasks=upstream_tasks,
    )

    wait_for_flow_run(
        flow_id,
        raise_final_state=True,
        task_args=dict(name=f"wait_for_flow_run({flow_name})"),
    )

with Flow(FLOW_NAME, schedule=schedule, executor=LocalDaskExecutor()) as flow:
    dbt_run_result = dbt.DbtCloudRunJob()(cause=FLOW_NAME, job_id=DBT_CLOUD_JOB_ID, wait_for_job_run_completion=True)
    data_result = execute_flow.map(DATA_FLOWS, upstream_tasks=[dbt_run_result])
    execute_flow(prospect_flow.FLOW_NAME, upstream_tasks=data_result)

# for execution in development only
if __name__ == "__main__":
    # flow.visualize()
    flow.run()
