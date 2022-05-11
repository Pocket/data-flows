from datetime import timedelta, datetime

from prefect import Flow, task, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.tasks.aws import StepActivate
from prefect.tasks.dbt import dbt
from prefect.tasks.prefect import create_flow_run

import flows.import_mysql
import flows.prospecting
from utils import config
from utils.flow import get_flow_name

FLOW_NAME = get_flow_name(__file__)
DBT_CLOUD_JOB_ID = 52822

# Flows to be executed before DBT modeling job.
IMPORT_MYSQL_FLOWS = [
    flows.import_mysql.curated_feed_flow,
    flows.import_mysql.curated_feed_prospects_flow,
    flows.import_mysql.tile_source_flow
]

# Flows to be run after the DBT job run has finished successfully.
DATA_FLOWS = [
    flows.prospecting.prereview_engagement_feature_store_flow,
    flows.prospecting.postreview_engagement_feature_store_flow,
]

ARN_PREFIX = 'arn:aws:states:us-east-1:996905175585:stateMachine:'
PROSPECTING_STEP_FUNCTIONS = [
    'TimespentProspectModelTrainingFlow'
]

if config.ENVIRONMENT == config.ENV_PROD:
    schedule = Schedule(clocks=[CronClock("0 0 * * *")])  # Nightly at midnight UTC
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
    """
    DBT Cloud jobs fail somewhat regularly. Retry to reduce debugging noise.
    """
    return dbt.DbtCloudRunJob()(cause=FLOW_NAME, job_id=DBT_CLOUD_JOB_ID, wait_for_job_run_completion=True)


@task()
def extract():
    return execute_flow.map([f.FLOW_NAME for f in IMPORT_MYSQL_FLOWS])


@task()
def load():
    return execute_flow.map([f.FLOW_NAME for f in DATA_FLOWS])


@task()
def execute_step(name, timestamp):
    arn = '{}{}'.format(ARN_PREFIX, name)
    execution_name = 'Prefect Prospect {name} {timestamp}'.format(name=name, timestamp=timestamp)
    return StepActivate(state_machine_arn=arn, execution_name=execution_name)


@task()
def prospect():
    """
    Run all prospecting step functions at the same time.
    """
    timestamp = datetime.now().strftime("%Y%m%d %H%M%S")
    return execute_step.map(PROSPECTING_STEP_FUNCTIONS, unmapped(timestamp))


with Flow(FLOW_NAME, schedule=schedule, executor=LocalDaskExecutor()) as flow:
    extract_result = extract()
    transform_result = transform(extract_result)
    load_result = load(transform_result)
    prospect(load_result)

# for execution in development only
if __name__ == "__main__":
    # flow.visualize()
    flow.run()
