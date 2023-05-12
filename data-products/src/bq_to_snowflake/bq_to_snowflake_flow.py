from prefect import flow, task
from glob import glob
import os

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))

@task()
def bq_staging(file_path: str):
    return "Hello World"

@task()
def bq_extract(input):
    return "Hello World"

@task()
def snowflake_load(input):
    return "Hello World"

@flow()
def bq_to_snowflake_etl(file_path: str):
    stg = bq_staging(file_path)
    ex = bq_extract(stg)
    snowflake_load(ex)

@flow()
def bq_to_snowflake():
    """Main Prefect flow for orchestrating the ingestion of
    specific Big Query data extractions into Snowflake.  Per author
    of the original Airflow DAG, each SQL file can run in a concurrent
    workflow.
    """
    for f in glob(os.path.join(SCRIPT_PATH, "sql/**/*.sql")):
        bq_to_snowflake_etl(f)

if __name__ == "__main__":
    bq_to_snowflake()