from prefect import flow, task

@task()
def bq_staging():
    pass

@task()
def bq_extract():
    pass

@task()
def snowflake_load():
    pass

@flow()
def bq_to_snowflake_etl():
    pass

@flow()
def bq_to_snowflake():
    pass