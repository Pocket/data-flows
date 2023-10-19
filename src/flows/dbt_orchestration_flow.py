import datetime
import os
from time import sleep

from prefect import Flow, task
from prefect.tasks.dbt import dbt
from prefect.tasks.dbt.dbt_cloud_utils import wait_for_job_run
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.executors import LocalDaskExecutor
from utils import config

from flows.braze import update_flow as braze_update_flow
from flows.prospecting import prereview_engagement_feature_store_flow, postreview_engagement_feature_store_flow
from utils.flow import get_flow_name, get_interval_schedule

FLOW_NAME = get_flow_name(__file__)
DBT_CLOUD_JOB_ID = 52822

# List of flow names that will be run after the Dbt job run has finished successfully.
DBT_DOWNSTREAM_FLOW_NAMES = [
    braze_update_flow.FLOW_NAME,
    prereview_engagement_feature_store_flow.FLOW_NAME,
    postreview_engagement_feature_store_flow.FLOW_NAME,
]


# Set max_retries to 1 because this flow has a long timeout.
# TODO: Set a concurrency-limit to prevent using more than one Dbt job resource.
@task(timeout=45 * 60)
def transform():
    run = dbt.DbtCloudRunJob().run(cause=FLOW_NAME, job_id=DBT_CLOUD_JOB_ID)
    # Prefect's wait_for_dbt_cloud_job_run immediately fires a request to Dbt to get the run status. If Dbt has not yet
    # created the run it returns 404: Not found, and Prefect raises an exception. Our workaround is to wait a bit here.
    sleep(30)
    wait_for_job_run(
        account_id=int(os.environ['DBT_CLOUD_ACCOUNT_ID']),
        token=os.environ['DBT_CLOUD_TOKEN'],
        run_id=run["id"],
        domain="cloud.getdbt.com",
    )


with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=60), executor=LocalDaskExecutor()) as flow:
    dbt_run_result = transform()

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
            task_args=dict(name=f"wait_for_flow_run({downstream_flow_name})"),
        )

# for execution in development only
if __name__ == "__main__":
    # flow.visualize()
    flow.run()
