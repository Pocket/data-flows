import os
from datetime import timedelta
from pathlib import Path, PosixPath
from unittest.mock import patch

from prefect import flow, task

from common.deployment import (
    CronSchedule,
    FlowDeployment,
    FlowEnvar,
    FlowSpec,
    IntervalSchedule,
    get_pyproject_metadata,
)

TEST_PYTHONPATH = os.path.expanduser(os.path.join(os.getcwd(), "tests/test_flows"))


@patch("common.deployment.FlowDeployment.push_deployment")
@patch("prefect_aws.ecs.ECSTask.save")
def test_flow_spec_disable_handle_task_definition(mock_ecs_save, mock_deployment):
    os.environ["POCKET_PREFECT_INFRASTRUCTURE_BLOCK"] = "-ib docker-container/test"

    # Test basic functionality of Flow Spec.
    # Mocking methods that make API calls to aid in testing.
    @task()
    def task_1():
        print("hello world")

    @flow()
    def flow_1():
        task_1()

    flow_spec = FlowSpec(
        flow=flow_1,
        docker_env="base",
        secrets=[
            FlowEnvar(
                envar_name="MY_SECRET_JSON", envar_value="/my/secretsmanager/secret"
            )
        ],
        envars=[
            FlowEnvar(
                envar_name="TEST", envar_value="test"
            )
        ],
        ephemeral_storage_gb=50,
        deployments=[
            FlowDeployment(
                deployment_name="base",
                cpu="1024",
                memory="4096",
                parameters={"param_name": "param_value"},
                schedule=CronSchedule(cron="0 0 * * *"),
            ),  # type: ignore
            FlowDeployment(
                deployment_name="hourly",
                cpu="1024",
                memory="4096",
                parameters={"param_name": "param_value"},
                schedule=IntervalSchedule(interval=timedelta(hours=1)),
            ),  # type: ignore
        ],  # type: ignore
    )
    flow_spec.push_deployments(
        Path("tests/test_flows/flow_group_1/example_flow.py"),
        get_pyproject_metadata(),
        "123456789012",
        object(),
        TEST_PYTHONPATH,
    )
    assert mock_deployment.call_count == 2
    assert mock_ecs_save.call_count == 0
    assert flow_1.name == "common-utils.flow-group-1.flow-1"
    mock_deployment.assert_called_with(
        "common-utils",
        "overridden",
        PosixPath("tests/test_flows/flow_group_1/example_flow.py"),
        "flow_1",
        "common-utils.flow-group-1.flow-1",
        TEST_PYTHONPATH,
        {"TEST": "test"}
    )
