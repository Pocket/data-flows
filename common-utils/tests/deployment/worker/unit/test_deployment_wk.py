import asyncio
import os
from unittest.mock import call, patch

import pytest
import yaml
from boto3 import session
from common.deployment.worker import (
    DEPLOYMENT_TYPE,
    FlowDeployment,
    FlowDockerEnv,
    FlowSpec,
    PrefectProject,
    get_ecs_image_name,
    get_ecs_task_arn,
    get_ecs_task_name,
    get_image_name,
    get_pyproject_metadata,
    run_command,
)
from moto import mock_ecs
from prefect import flow


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


def test_helpers():
    # test our basic helpers
    x = get_image_name("test", "test_v2")
    assert x == "test-test-v2"
    x = get_ecs_task_name("test", "test_v2")
    assert x == f"test-test-v2-{DEPLOYMENT_TYPE}"
    x = get_ecs_image_name("test", "test_v2")
    assert (
        x
        == f"123456789.dkr.ecr.us-east-1.amazonaws.com/data-flows-prefect-v2-envs:test-test-v2-{DEPLOYMENT_TYPE}"  # noqa: E501
    )
    x = get_ecs_task_arn("test", "test_v2")
    assert (
        x
        == f"arn:aws:ecs:us-east-1:123456789:task-definition/test-test-v2-{DEPLOYMENT_TYPE}"  # noqa: E501
    )


@mock_ecs
@patch("common.deployment.worker.run_command")
def test_flow_docker_env(mock_cmd):
    # full test of the FlowDockerEnv model
    mock_cmd.return_value = "2.14.9"
    x = FlowDockerEnv(
        env_name="test-v1",
        dockerfile_path="tests/deployment/unit/testDockerfile1",
        dependency_group="main",
        python_version="3.9",
        project_name="test",
    )
    assert mock_cmd.call_args_list == [
        call(
            os.path.join(
                os.getcwd(), "src/common/deployment/worker/get_prefect_version.sh main"
            )  # noqa: E501
        )
    ]
    mock_cmd.reset_mock()
    assert x.image_name == "test-test-v1"
    assert x.ecs_task_name == f"test-test-v1-{DEPLOYMENT_TYPE}"
    assert (
        x.ecs_image_name
        == f"123456789.dkr.ecr.us-east-1.amazonaws.com/data-flows-prefect-v2-envs:test-test-v1-{DEPLOYMENT_TYPE}"  # noqa: E501
    )  # noqa: E501
    x.build_image()
    assert mock_cmd.call_args_list == [
        call(
            os.path.join(
                os.getcwd(),
                "src/common/deployment/worker/build_image.sh test-test-v1 tests/deployment/unit/testDockerfile1 main 3.9 2.14.9",  # noqa: E501
            )
        )
    ]
    mock_cmd.reset_mock()
    x.push_image()
    assert mock_cmd.call_args_list == [
        call(
            os.path.join(
                os.getcwd(),
                "src/common/deployment/worker/push_aws_image.sh test-test-v1 123456789 us-east-1 dev",  # noqa: E501
            )
        )
    ]
    ecs_client = session.Session().client("ecs")
    task_def_arn = x._handle_ecs_task_definition(ecs_client)
    assert (
        task_def_arn
        == "arn:aws:ecs:us-east-1:123456789012:task-definition/test-test-v1-dev:1"
    )
    # run again to test that new revision is created
    task_def_arn = x._handle_ecs_task_definition(ecs_client)
    assert (
        task_def_arn
        == "arn:aws:ecs:us-east-1:123456789012:task-definition/test-test-v1-dev:1"
    )
    # patch GIT_SHA to force revision
    with patch("common.deployment.worker.GIT_SHA", "1234567"):
        task_def_arn = x._handle_ecs_task_definition(ecs_client)
        assert (
            task_def_arn
            == "arn:aws:ecs:us-east-1:123456789012:task-definition/test-test-v1-dev:2"
        )
    # patch DEFAULT_CPU to force revision
    with patch("common.deployment.worker.DEFAULT_CPU", "1024"):
        task_def_arn = x._handle_ecs_task_definition(ecs_client)
        assert (
            task_def_arn
            == "arn:aws:ecs:us-east-1:123456789012:task-definition/test-test-v1-dev:3"
        )


@mock_ecs
@patch("common.deployment.worker.run_command")
def test_prefect_project(mock_cmd):
    # full test of PrefectProject model
    mock_cmd.return_value = "2.14.9"
    e1 = FlowDockerEnv(
        env_name="test-v1",
        dockerfile_path="tests/deployment/unit/testDockerfile1",
        dependency_group="main",
        python_version="3.9",
        project_name="test",
    )
    e2 = FlowDockerEnv(
        env_name="test-v2",
        dockerfile_path="tests/deployment/unit/testDockerfile1",
        dependency_group="special",
        python_version="3.10",
        project_name="test",
    )
    x = PrefectProject(project_name="test", docker_envs=[e1, e2])
    envs = x.docker_env_keys
    assert envs == ["test-v1", "test-v2"]
    x.clone_project("test", "test", "tmp")
    assert mock_cmd.call_args_list == [
        call(
            os.path.join(
                os.getcwd(), "src/common/deployment/worker/get_prefect_version.sh main"
            )
        ),
        call(
            os.path.join(
                os.getcwd(),
                "src/common/deployment/worker/get_prefect_version.sh special",
            )
        ),
        call(
            os.path.join(
                os.getcwd(),
                "src/common/deployment/worker/clone_project.sh test test test tmp",
            )
        ),
    ]
    mock_cmd.reset_mock()
    x.process_docker_envs()
    assert mock_cmd.call_args_list == [
        call(
            os.path.join(
                os.getcwd(),
                "src/common/deployment/worker/build_image.sh test-test-v1 tests/deployment/unit/testDockerfile1 main 3.9 2.14.9",  # noqa: E501
            )
        ),
        call(
            os.path.join(
                os.getcwd(),
                "src/common/deployment/worker/push_aws_image.sh test-test-v1 123456789 us-east-1 dev",  # noqa: E501
            )
        ),
        call(
            os.path.join(
                os.getcwd(),
                "src/common/deployment/worker/build_image.sh test-test-v2 tests/deployment/unit/testDockerfile1 special 3.10 2.14.9",  # noqa: E501
            )
        ),
        call(
            os.path.join(
                os.getcwd(),
                "src/common/deployment/worker/push_aws_image.sh test-test-v2 123456789 us-east-1 dev",  # noqa: E501
            )
        ),
    ]
    mock_cmd.reset_mock()
    mock_cmd.return_value = "https://github.com/Pocket/data-flows.git"
    with pytest.raises(Exception) as e:
        # this has failures because of test flows and is expected.
        asyncio.run(x.process_flow_specs())
        assert mock_cmd.call_args_list == [call("git ls-remote --get-url origin")]
        assert "The following flows failed validation: ['example_flow']" in str(e.value)


@mock_ecs
@pytest.mark.parametrize("deployment_type", ["dev", "staging", "main"])
@patch("common.deployment.worker.run_command")
def test_flow_specs(mock_cmd, deployment_type):
    with patch("common.deployment.worker.DEPLOYMENT_TYPE", deployment_type):
        # test all test flows
        mock_cmd.return_value = "2.14.9"
        x = get_pyproject_metadata()
        x.process_docker_envs()
        mock_cmd.reset_mock()
        mock_cmd.return_value = "https://github.com/Pocket/data-flows.git"
        with pytest.raises(Exception) as e:
            # this has failures because of test flows and is expected.
            asyncio.run(x.process_flow_specs())
            assert "The following flows failed validation: ['example_flow']" in str(
                e.value
            )
        with open("prefect.yaml") as f:
            data = yaml.load(f, Loader=yaml.SafeLoader)

        test_data = {
            "deployments": [
                {
                    "description": "flow",
                    "enforce_parameter_schema": False,
                    "entrypoint": "tests/deployment/worker/test_flows/example_flow.py:flow_1",  # noqa: E501
                    "name": f"base-{deployment_type}",
                    "parameters": {"param_name": "param_value"},
                    "schedule": {"cron": "0 0 * * *", "timezone": "UTC"},
                    "tags": ["test"],
                    "version": "1",
                    "work_pool": {
                        "job_variables": {
                            "cpu": "1024",
                            "memory": "4096",
                            "task_definition_arn": f"arn:aws:ecs:us-east-1:123456789:task-definition/common-utils-base-v2-{deployment_type}",  # noqa: E501
                        },
                        "name": f"mozilla-aws-ecs-fargate-{deployment_type}",
                        "work_queue_name": "default",
                    },
                },
                {
                    "description": "flow",
                    "enforce_parameter_schema": False,
                    "entrypoint": "tests/deployment/worker/test_flows/single_flow/example_flow.py:flow_1",  # noqa: E501
                    "name": f"test-{deployment_type}",
                    "parameters": {"param_name": "param_value"},
                    "schedule": {"cron": "0 0 * * *", "timezone": "UTC"},
                    "tags": ["test"],
                    "version": "1",
                    "work_pool": {
                        "job_variables": {
                            "cpu": "1024",
                            "memory": "4096",
                            "task_definition_arn": f"arn:aws:ecs:us-east-1:123456789:task-definition/common-utils-base-v2-{deployment_type}",  # noqa: E501
                        },
                        "name": f"mozilla-aws-ecs-fargate-{deployment_type}",
                        "work_queue_name": "default",
                    },
                },
            ],
            "name": "common-utils",
            "pull": [
                {
                    "prefect.deployments.steps.run_shell_script": {
                        "id": "clone_project",
                        "script": "df-cli clone-project https://github.com/Pocket/data-flows.git dev-v2",  # noqa: E501
                        "stream_output": True,
                    }
                },
                {
                    "prefect.deployments.steps.set_working_directory": {
                        "directory": "/opt/prefect/common-utils"
                    }
                },
            ],
        }
        if deployment_type != "main":
            for i in test_data["deployments"]:
                del i["schedule"]
        assert data == test_data


@mock_ecs
@patch("common.deployment.worker.run_command")
@patch("common.deployment.worker.SourceFileLoader")
def test_flow_specs_exception(mock_loader, mock_cmd):
    # need to check that Exception catching works
    mock_cmd.return_value = "2.14.9"
    mock_loader.side_effect = Exception("I am an exception!")
    x = get_pyproject_metadata()
    x.process_docker_envs()
    mock_cmd.reset_mock()
    mock_cmd.return_value = "https://github.com/Pocket/data-flows.git"
    with pytest.raises(Exception) as e:
        asyncio.run(x.process_flow_specs())
        assert "The following flows failed validation: ['example_flow']" in str(e.value)


@mock_ecs
@patch("common.deployment.worker.run_command")
def test_flow_spec_elements(mock_cmd):
    # check FlowSpec directly
    @flow(name="name_directly")
    def test_flow():
        return "hello world"

    FLOW_SPEC = FlowSpec(
        flow=test_flow,
        docker_env="base_v2",
        deployments=[
            FlowDeployment(
                name="base",
                parameters={"param_name": "param_value"},
                cron="0 0 * * *",
                description="flow",
                version="1",
                tags=["test"],
                job_variables={
                    "cpu": "1024",
                    "memory": "4096",
                },
            )
        ],  # type: ignore
    )
    assert test_flow.name == "common-utils.name_directly"
    assert FLOW_SPEC.deployments[0].work_pool_name == "mozilla-aws-ecs-fargate"

    @flow()
    def test_flow_2():
        return "hello world"

    FLOW_SPEC = FlowSpec(
        flow=test_flow_2,
        docker_env="base_v2",
        deployments=[FlowDeployment(name="base")],
    )
    assert test_flow_2.name == "common-utils.test-flow-2"
