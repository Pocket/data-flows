from datetime import timedelta

from prefect import Flow, task
from prefect.executors import LocalDaskExecutor
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.tasks.dbt import dbt
from prefect.tasks.prefect import create_flow_run

from flows.import_mysql import curated_feed_flow, curated_feed_prospects_flow, tile_source_flow
from flows.prospecting import prereview_engagement_feature_store_flow, postreview_engagement_feature_store_flow, \
    prospect_flow
from utils import config
from utils.flow import get_flow_name

FLOW_NAME = get_flow_name(__file__)
DBT_CLOUD_JOB_ID = 52822

# List of flow names that will be run after the Dbt job run has finished successfully.
DATA_FLOWS = [
    prereview_engagement_feature_store_flow.FLOW_NAME,
    postreview_engagement_feature_store_flow.FLOW_NAME,
    prospect_flow
]

IMPORT_MYSQL_FLOWS = [
    curated_feed_flow,
    curated_feed_prospects_flow,
    tile_source_flow
]

if config.ENVIRONMENT == config.ENV_PROD:
    schedule = Schedule(clocks=[CronClock("0 0 * * *")]) # Nightly at midnight UTC
else:
    schedule = None

@task()
def execute_flow(flow_name):
    return create_flow_run(
        flow_name=flow_name,
        project_name=config.PREFECT_PROJECT_NAME,
        task_args=dict(name=f"create_flow_run({flow_name})"),
        wait=True
    )

@task(max_retries=3, retry_delay=timedelta(minutes=1))
def transform():
    return dbt.DbtCloudRunJob()(cause=FLOW_NAME, job_id=DBT_CLOUD_JOB_ID, wait_for_job_run_completion=True)

@task()
def extract():
    return execute_flow.map(IMPORT_MYSQL_FLOWS)

@task()
def load():
    return execute_flow.map(DATA_FLOWS)

@task()
def prospect():
    execute_flow(prospect_flow.FLOW_NAME)

with Flow(FLOW_NAME, schedule=schedule, executor=LocalDaskExecutor()) as flow:
    extract_result = extract()
    transform_result = transform(extract)
    load_result = load(transform_result)
    prospect(load_result)

# for execution in development only
if __name__ == "__main__":
    # flow.visualize()
    flow.run()
