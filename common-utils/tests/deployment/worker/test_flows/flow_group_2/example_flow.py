from common.deployment.worker import FlowDeployment, FlowSpec
from prefect import flow, task


@task()
def task_2():
    print("hello world")


@flow()
def flow_2():
    task_2()


FLOW_SPEC = FlowSpec(
    flow=flow_2,
    docker_env="bad docker env",
    deployments=[
        FlowDeployment(
            name="base",
        )
    ],
)
