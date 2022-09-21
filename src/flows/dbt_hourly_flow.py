from prefect import Flow
from prefect.executors import LocalDaskExecutor
from prefect.tasks.dbt import dbt

from utils.flow import get_flow_name, get_interval_schedule

FLOW_NAME = get_flow_name(__file__)
DBT_CLOUD_JOB_ID = 52822

with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=30), executor=LocalDaskExecutor()) as flow:
    dbt.DbtCloudRunJob().run(cause=FLOW_NAME, job_id=DBT_CLOUD_JOB_ID, wait_for_job_run_completion=True)

if __name__ == "__main__":
    flow.run()
