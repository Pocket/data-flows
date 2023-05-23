from common.deployment import FlowDeployment, FlowSpec
from prefect import flow, get_run_logger, task
from shared.blocks.notifications.pagerduty import get_notification_block


@task
def orchestrate_flows():
    logger = get_run_logger()
    logger.info("Orchestrating some flows yall!")


@flow
def main_orchestration_flow():
    orchestrate_flows()
    logger = get_run_logger()
    logger.info(type(get_notification_block))


FLOW_SPEC = FlowSpec(
    flow=main_orchestration_flow,
    docker_env="base",
    ephemeral_storage_gb=200,
    deployments=[
        FlowDeployment(deployment_name="base", cpu="1024", memory="4096")  # type: ignore
    ],
)

if __name__ == "__main__":
    main_orchestration_flow()
