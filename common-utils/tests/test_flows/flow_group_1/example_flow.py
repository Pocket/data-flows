from prefect import flow, task
from prefect.server.schemas.schedules import CronSchedule

from common.deployment import FlowDeployment, FlowEnvar, FlowSpec


@task()
def task_1():
    print("hello world")


@flow(log_prints=True)
def flow_1(param_name: str):
    print(param_name)
    task_1()


FLOW_SPEC = FlowSpec(
    flow=flow_1,
    docker_env="base",
    secrets=[
        FlowEnvar(
            envar_name="MY_SECRET_JSON", envar_value="dpt/dev/data_flows_prefect_test"
        )
    ],
    ephemeral_storage_gb=200,
    deployments=[
        FlowDeployment(
            deployment_name="base",
            cpu="1024",
            memory="4096",
            parameters={"param_name": "param_value"},
            schedule=CronSchedule(cron="0 0 * * *"),
        )
    ],  # type: ignore
)
