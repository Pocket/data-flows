from prefect import Flow, task, unmapped
from prefect.tasks.dbt import dbt
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.executors import LocalDaskExecutor
from utils import config

from flows.braze import update_flow as braze_update_flow
from flows.prospecting import prereview_engagement_feature_store_flow, postreview_engagement_feature_store_flow
from flows import dbt_hourly_flow
from utils.flow import get_flow_name, get_interval_schedule

FLOW_NAME = get_flow_name(__file__)
DBT_CLOUD_JOB_ID = 52822

# List of flow names that will be run after the Dbt job run has finished successfully.
EXTRACT_FLOWS = [
]

PRE_ENRICH_FLOWS = [
]

TRANSFORM_FLOWS = [
    flows.dbt_hourly_flow,
]

LOAD_FLOWS = [
    braze_update_flow.FLOW_NAME,
    prereview_engagement_feature_store_flow.FLOW_NAME,
    postreview_engagement_feature_store_flow.FLOW_NAME,
]

@task(timeout=15 * 60)
def transform():
    return dbt.DbtCloudRunJob().run(cause=FLOW_NAME, job_id=DBT_CLOUD_JOB_ID, wait_for_job_run_completion=True)


with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=30), executor=LocalDaskExecutor()) as flow:
    dbt_run_result = transform()

    load_results = create_flow_run.map(
        flow_name=LOAD_FLOWS,
        project_name=unmapped(config.PREFECT_PROJECT_NAME),
        upstream_tasks=[dbt_run_result],
    )

    wait_for_flow_run.map(
        load_results,
        raise_final_state=unmapped(True),
    )

# for execution in development only
if __name__ == "__main__":
    # flow.visualize()
    flow.run()
