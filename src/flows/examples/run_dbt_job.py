from prefect.tasks.dbt import dbt
from prefect import task, Flow
from prefect.triggers import all_successful
import prefect

DBT_CLOUD_JOB_ID = 52822

@task(trigger=all_successful)
def log_task(message):
    logger = prefect.context.get("logger")
    logger.info(f'{message}')

@task(trigger=all_successful)
def run_dbt_job(cause: str, job_id: int) -> dict:
    return dbt.DbtCloudRunJob().run(cause=cause, job_id=job_id, wait_for_job_run_completion=True)

with Flow("dbt_job_run") as flow:
    start_flow_task = log_task('Starting Test DBT job')
    dbt_run_result = run_dbt_job(cause='Test DBT Run', job_id=DBT_CLOUD_JOB_ID)
    end_flow_task = log_task(f'Ending Test DBT job\n{dbt_run_result}')
    dbt_run_result.set_upstream(start_flow_task)
    dbt_run_result.set_downstream(end_flow_task)

# for execution in development only
if __name__ == "__main__":
    # flow.visualize()
    flow.run()
