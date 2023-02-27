from unittest.mock import patch

import pytest
from moto import mock_sts

from common.deployment import (
    FlowDockerEnv,
    ProjectEnv,
    get_aws_account_id,
    process_project_env,
    run_command,
)


@mock_sts
def test_get_aws_account_id():
    get_aws_account_id()


def test_run_command():
    x = run_command(
        """echo "this is a 
    multiline command that returns
    the last line"
    """
    )
    assert x.strip() == "the last line"


def test_run_command_execption():
    with pytest.raises(Exception):
        run_command(
            """bad-command "this is a 
        multiline command that returns
        the last line"
        """
        )


@patch("common.deployment.run_command")
def test_flow_docker_env(mock_cmd):
    x = FlowDockerEnv(
        project_name="test",
        env_name="test",
        dockerfile_path="tests/unit/deployment/testDockerfile",
        docker_build_context="tests/unit/deployment",
        python_version="3.10",
    )
    x.build_image()
    x.push_image("12345")
    assert mock_cmd.call_count == 2


@mock_sts
@patch("common.deployment.run_command")
@patch("common.deployment.S3")
def test_flow_project_env(mock_cmd, mock_s3):
    x = ProjectEnv(
        project_name="test",
        docker_envs=[
            FlowDockerEnv(
                project_name="test",
                env_name="test",
                dockerfile_path="tests/unit/deployment/testDockerfile",
                docker_build_context="tests/unit/deployment",
                python_version="3.10",
            )
        ],
    )
    x.process()
    assert mock_s3.call_count == 2
    assert mock_cmd.call_count == 2


@mock_sts
@patch("common.deployment.run_command")
@patch("common.deployment.S3")
def test_process_project_env(mock_cmd, mock_s3):
    process_project_env()
    assert mock_s3.call_count == 2
    assert mock_cmd.call_count == 2
