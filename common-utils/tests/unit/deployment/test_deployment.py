from pathlib import Path
from unittest.mock import patch

import pytest
from moto import mock_sts
from prefect import flow, task

from common.deployment import (
    LOGGER_NAME,
    CronSchedule,
    FlowDeployment,
    FlowDockerEnv,
    FlowSpec,
    IntervalSchedule,
    PrefectProject,
    RRuleSchedule,
    get_aws_account_id,
    get_flow_folder,
    run_command,
)


@mock_sts
def test_get_aws_account_id():
    # Test sts call using moto sts mock.
    get_aws_account_id()


def test_run_command():
    # Simple test of run_command.
    # Validate that last line is returned as expected.
    x = run_command(
        """echo "this is a 
    multiline command that returns
    the last line"
    """
    )
    assert x.strip() == "the last line"


def test_run_command_execption():
    # Validate the run_command throws proper exception.
    with pytest.raises(Exception):
        run_command(
            """bad-command "this is a 
        multiline command that returns
        the last line"
        """
        )


def test_get_flow_folder():
    # Validate function returns slugified value.
    path = Path("test_test/1_2_3/test.py")
    x = get_flow_folder(path)
    assert x == "1-2-3"


@patch("common.deployment.run_command")
def test_flow_docker_env(mock_cmd):
    # Validate class methods using mock on run_command.
    x = FlowDockerEnv(
        project_name="test",
        env_name="test",
        dockerfile_path=Path("tests/unit/deployment/testDockerfile"),
        docker_build_context=Path("tests/unit/deployment"),
        python_version="3.10",
    )
    x.build_image()
    x.push_image("12345")
    assert mock_cmd.call_count == 2


@patch("common.deployment.run_command")
def test_flow_deployment(mock_cmd):
    # Test schedule method return cli arg as expected.
    d1 = FlowDeployment(deployment_name="test", schedule=CronSchedule(cron="0 0 * * *"))  # type: ignore
    x1 = d1._get_schedule_arg()
    assert x1 == "--cron 0 0 * * *"
    d2 = FlowDeployment(deployment_name="test", schedule=IntervalSchedule(interval=60))  # type: ignore
    x2 = d2._get_schedule_arg()
    assert x2 == "--interval 60"
    d3 = FlowDeployment(deployment_name="test", schedule=RRuleSchedule(rrule="FREQ=HOURLY;BYDAY=MO,TU,WE,TH,FR;BYHOUR=9,10,11,12,13,14,15,16,17"))  # type: ignore
    x3 = d3._get_schedule_arg()
    assert (
        x3
        == "--rrule FREQ=HOURLY;BYDAY=MO,TU,WE,TH,FR;BYHOUR=9,10,11,12,13,14,15,16,17"
    )
    d4 = FlowDeployment(deployment_name="test")  # type: ignore
    x4 = d4._get_schedule_arg()
    assert x4 == ""

    # Validate push deployment method using mock on run_command.
    d5 = FlowDeployment(
        deployment_name="test",
        schedule=IntervalSchedule(interval=120),  # type: ignore
        cpu="1024",
        memory="4096",
        parameters={"test_param": "test_value"},
    )
    d5.push_deployment(
        project_name="test-project",
        storage_path="test-bucket/test-folder",
        infrastructure="test-ECS-block",
        flow_path=Path("tests/unit/deployment/test_deployment.py"),
        flow_function_name="test_function",
        skip_upload=True,
    )
    assert mock_cmd.call_count == 1
    call_text = """export PREFECT_PYPROJECT_PATH=/users/mozilla/projects/data-flows/common-utils/pyproject.toml && \\\n        pushd tests/unit/deployment && \\\n        prefect deployment build test_deployment.py:test_function \\\n        -n test \\\n        -sb s3/test-bucket/test-folder \\\n        -ib ecs-task/test-ECS-block \\\n        --override cpu=1024 --override memory=4096 \\\n        -q prefect-v2-queue-dev-test \\\n        -v dev \\\n        --params \'{"test_param": "test_value"}\' \\\n        -t test-project -t deployment \\\n        -a \\\n        --interval 120 --skip-upload && \\\n        popd"""
    mock_cmd.assert_called_with(call_text)


@mock_sts
@patch("common.deployment.FlowDeployment.push_deployment")
@patch("common.deployment.ECSTask.save")
@patch("common.deployment.ECSTask.load")
@patch("common.deployment.S3.load")
def test_flow_spec(mock_deployment, mock_ecs_save, mock_ecs_load, mock_s3_load):
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
        deployments=[FlowDeployment(deployment_name="base")],  # type: ignore
    )
    flow_spec.push_deployments(Path("tests/test_flows/flow_group_1/example_flow.py"))
    assert mock_deployment.call_count == 1
    assert mock_s3_load.call_count == 1
    assert mock_ecs_load.call_count == 1
    assert mock_ecs_save.call_count == 1
    assert flow_1.name == "common-utils.flow-1"


@patch("common.deployment.DISABLE_FLOW_SPEC", "true")
def test_flow_spec_no_validate():
    # Validate that FlowSpec validation disable works.
    FlowSpec(
        flow="not a flow object",
        docker_env="bad env",
        deployments=[FlowDeployment(deployment_name="base")],  # type: ignore
    )


def test_flow_spec_bad_docker_env():
    @task()
    def task_2():
        print("hello world")

    @flow()
    def flow_2():
        task_2()

    with pytest.raises(Exception):
        FLOW_SPEC = FlowSpec(
            flow=flow_2,
            docker_env="bad env",
            deployments=[FlowDeployment(deployment_name="base")],  # type: ignore
        )


@mock_sts
@patch("common.deployment.run_command")
@patch("common.deployment.S3")
@patch("common.deployment.FlowSpec.push_deployments")
def test_flow_project_env(mock_cmd, mock_s3, mock_deployment):
    # Test the PrefectProject class, which is called by the cli
    # Mocking methods that make API calls to aid in testing.
    # This uses the test configuration in pyproject.toml.
    # 2 of the test flow files will purposely trigger except sections.
    # 1 of these is bad because the Spec is not there.
    # That is why calls are 2 and not 3.
    # Ignore the warnings for now.
    # This has to do with how Prefect caches things I believe.
    with pytest.raises(Exception):
        x = PrefectProject()
        x.process_project_docker_envs()
        x.process_project_flows()
        assert mock_s3.call_count == 2
        assert mock_cmd.call_count == 2
        assert mock_deployment.call_count == 2


@mock_sts
@patch("common.deployment.run_command")
def test_flow_project_env_validate_build_only(mock_cmd):
    # Same as test_flow_project_env, except we are doing validate and build only.
    with pytest.raises(Exception):
        x = PrefectProject()
        x.process_project_docker_envs(build_only=True)
        x.process_project_flows(validate_only=True)
    assert mock_cmd.call_count == 1


@mock_sts
@patch("common.deployment.FlowSpec.push_deployments")
def test_flow_project_env_inner_exception(mock_deployment):
    # Same as test_flow_project_env, except we are doing validate and build only.
    mock_deployment.side_effect = Exception("misc")
    with pytest.raises(Exception):
        x = PrefectProject()
        x.process_project_flows()
    assert mock_deployment.call_count == 1


@mock_sts
@patch("common.deployment.FlowSpec")
def test_flow_project_env_all_pass(mock_deployment):
    # Same as test_flow_project_env, except we force FlowSpec methods to pass.
    x = PrefectProject()
    x.process_project_flows()
    assert mock_deployment.call_count == 2
