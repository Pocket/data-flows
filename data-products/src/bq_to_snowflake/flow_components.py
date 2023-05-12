import os
import json
from prefect import flow, task

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
def bq_to_snowflake():
        stg = bq_staging("test")
        ex = bq_extract(stg)
        snowflake_load(ex)


def subflow_factory(group_id: str):
    with open("src/bq_to_snowflake/config/bq_to_snowflake.json") as f:
        groups = json.load(f)
    group = groups[group_id]
    for s in group["subflow_set_defs"]:
         bq_to_snowflake()
    
