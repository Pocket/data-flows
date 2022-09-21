from prefect import Flow
from prefect.tasks.dbt import dbt

from utils.flow import get_flow_name

FLOW_NAME = get_flow_name(__file__)
DBT_CLOUD_JOB_ID = 52822

with Flow(FLOW_NAME) as flow:
    dbt.DbtCloudRunJob().run(cause=FLOW_NAME, job_id=DBT_CLOUD_JOB_ID, wait_for_job_run_completion=True)

if __name__ == "__main__":
    flow.run()
