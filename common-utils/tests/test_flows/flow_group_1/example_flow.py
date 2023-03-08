from prefect import flow, task
from prefect.server.schemas.schedules import CronSchedule

from common.deployment import FlowDeployment, FlowSecret, FlowSpec


@task()
def task_1():
    print("hello world")


@flow()
def flow_1():
    task_1()


FLOW_SPEC = FlowSpec(
    flow=flow_1,
    docker_env="base",
    secrets=[
        FlowSecret(envar_name="MY_SECRET_JSON", secret_name="/my/secretsmanager/secret")
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
