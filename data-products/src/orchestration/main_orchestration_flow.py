from common.deployment import FlowDeployment, FlowSpec, FlowEnvar
from prefect import flow, get_run_logger, task


@task
def orchestrate_flows():
    logger = get_run_logger()
    logger.info("Orchestrating some flows yall!")


@flow
def main_orchestration_flow():
    orchestrate_flows()


FLOW_SPEC = FlowSpec(
    flow=main_orchestration_flow,
    docker_env="base",
    ephemeral_storage_gb=200,
    secrets= [
    ],
    deployments=[
        FlowDeployment(deployment_name="base", cpu="1024", memory="4096", envars=FlowEnvar(
    envar_name="test",
    envar_value="test"
        ))  # type: ignore
    ],
)

if __name__ == "__main__":
    main_orchestration_flow()
