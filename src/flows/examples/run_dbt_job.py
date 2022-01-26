from prefect.tasks.dbt import dbt
from prefect import task, Flow
from prefect.triggers import all_successful
import prefect

DBT_CLOUD_JOB_ID = 52822

@task
def run_before_dbt_job():
    logger = prefect.context.get("logger")
    logger.info(f'Starting Test DBT job')

@task
def run_after_dbt_job():
    logger = prefect.context.get("logger")
    logger.info(f'Ending Test DBT job')

with Flow("dbt_job_run") as flow:
    before_dbt_job_task = run_before_dbt_job()
    dbt_run_result = dbt.DbtCloudRunJob()(cause='Test DBT Run', job_id=DBT_CLOUD_JOB_ID, wait_for_job_run_completion=True)
    after_dbt_job_task = run_after_dbt_job()

    # Define dependencies between the above tasks explicitely with "set_upstream" and "set_downstream".
    # Implicitily dependencies: If a return value of a prior task is used as an argument in a subsequent task,
    # Prefect would automatically infer the task dependencies.
    dbt_run_result.set_upstream(before_dbt_job_task)
    dbt_run_result.set_downstream(after_dbt_job_task)

# for execution in development only
if __name__ == "__main__":
    # flow.visualize()
    flow.run()
