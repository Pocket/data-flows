from common.deployment import FlowSpec, FlowDeployment
from prefect import task, flow
from prefect.server.schemas.schedules import CronSchedule


@task()
def task_1():
    print("hello world")


@flow()
def flow_1():
    task_1()


FLOW_SPEC = FlowSpec(
    flow=flow_1, docker_env="base", deployments=[FlowDeployment(deployment_name="base")]
)
