from prefect.tasks.dbt import dbt
from prefect import Flow

FLOW_NAME = "DBT Job Flow"
DBT_CLOUD_JOB_ID = 52822

with Flow(FLOW_NAME) as flow:
    dbt_run_result = dbt.DbtCloudRunJob()(cause='Test DBT Run', job_id=DBT_CLOUD_JOB_ID, wait_for_job_run_completion=True)

# for execution in development only
if __name__ == "__main__":
    flow.run()
