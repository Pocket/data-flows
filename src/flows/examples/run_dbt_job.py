from prefect.tasks.dbt import dbt
from prefect import task, Flow
from prefect.triggers import all_successful
import prefect

DBT_CLOUD_JOB_ID = 52822

@task
def run_before_dbt_job():
    logger = prefect.context.get("logger")
    logger.info(f'Starting Test DBT job')

@task(trigger=all_successful)
def run_dbt_job(cause: str, job_id: int) -> dict:
    return dbt.DbtCloudRunJob().run(cause=cause, job_id=job_id, wait_for_job_run_completion=True)

@task(trigger=all_successful)
def run_after_dbt_job():
    logger = prefect.context.get("logger")
    logger.info(f'Ending Test DBT job')

with Flow("dbt_job_run") as flow:
    before_dbt_job_task = run_before_dbt_job()
    dbt_run_result = run_dbt_job(cause='Test DBT Run', job_id=DBT_CLOUD_JOB_ID)
    after_dbt_job_task = run_after_dbt_job()
    dbt_run_result.set_upstream(before_dbt_job_task)
    dbt_run_result.set_downstream(after_dbt_job_task)

# for execution in development only
if __name__ == "__main__":
    # flow.visualize()
    flow.run()
