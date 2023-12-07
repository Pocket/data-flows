from unittest.mock import AsyncMock, MagicMock, patch

from common.deployment.worker import PrefectProject
from common.deployment.worker.cli import (
    build_common_utils,
    callback,
    check_version,
    clone_project,
    process_docker_envs,
    process_flow_specs,
)


@patch("common.deployment.worker.cli.cv")
def test_check_version(mock_cmd):
    check_version()
    assert mock_cmd.call_count == 1


@patch("common.deployment.worker.cli.run_command")
def test_build_common_utils(mock_cmd):
    build_common_utils()
    assert mock_cmd.call_count == 1


@patch("common.deployment.worker.cli.get_pyproject_metadata")
def test_clone_project(mock_cmd):
    clone_project("test", "test")
    assert mock_cmd.call_count == 1


@patch("common.deployment.worker.cli.get_pyproject_metadata")
def test_process_docker_envs(mock_cmd):
    process_docker_envs()
    assert mock_cmd.call_count == 1


@patch("common.deployment.worker.cli.get_pyproject_metadata")
def test_process_flows(mock_cmd):
    mock_method = AsyncMock()
    mock_cmd.return_value.process_flow_specs.side_effect = mock_method
    process_flow_specs()
    assert mock_cmd.call_count == 1


def test_callback():
    callback()
