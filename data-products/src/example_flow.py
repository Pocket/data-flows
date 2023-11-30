from common.deployment.worker import FlowDeployment, FlowSpec
from prefect import flow


@flow(name="examples.main-flow")
def main_flow():
    print("hello world")


FLOW_SPEC = FlowSpec(
    flow=main_flow,
    docker_env="base_v2",
    deployments=[
        FlowDeployment(name="base", tags=[])
    ],
)
