from common.deployment.worker import FlowDeployment, FlowSpec
from prefect import flow, task


@task()
def task_1():
    print("hello world")


@flow(log_prints=True)
def flow_1(param_name: str):
    print(param_name)
    task_1()


FLOW_SPEC = FlowSpec(
    flow=flow_1,
    docker_env="base_v2",
    is_agent=True,
    deployments=[
        FlowDeployment(
            name="base",
            parameters={"param_name": "param_value"},
            cron="0 0 * * *",
            job_variables={
                "cpu": "1024",
                "memory": "4096",
            },
        )
    ],  # type: ignore
)
