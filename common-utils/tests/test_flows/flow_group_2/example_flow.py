from prefect import flow, task

from common.deployment import FlowDeployment, FlowSpec


@task()
def task_2():
    print("hello world")


@flow()
def flow_2():
    task_2()


FLOW_SPEC = FlowSpec(
    flow=flow_2,
    docker_env="bad docker env",
    deployments=[FlowDeployment(deployment_name="base")],  # type: ignore
)
