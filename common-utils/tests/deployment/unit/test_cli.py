from unittest.mock import patch

from common.deployment.cli import main, parse_args


def test_parse_args():
    x = parse_args(["deploy-flows", "--validate-only"])
    assert x.validate_only
    assert x.subparser_name == "deploy-flows"
    x = parse_args(["deploy-envs", "--build-only"])
    assert x.build_only
    assert x.subparser_name == "deploy-envs"
    x = parse_args([])
    assert not x.subparser_name


@patch("sys.argv", ["deploy-cli", "deploy-envs", "--build-only"])
@patch("common.deployment.cli.PrefectProject.process_project_docker_envs")
def test_main_envs(mock_process):
    main()
    mock_process.assert_called_with(True)


@patch("sys.argv", ["deploy-cli", "deploy-flows", "--validate-only"])
@patch("common.deployment.cli.PrefectProject.process_project_flows")
def test_main_flows(mock_process):
    main()
    mock_process.assert_called_with(True)


@patch("sys.argv", ["deploy-cli", "deploy-envs"])
@patch("common.deployment.cli.PrefectProject.process_project_docker_envs")
def test_main_envs_run(mock_process):
    main()
    assert mock_process.called_with(False)


@patch("sys.argv", ["deploy-cli", "deploy-flows"])
@patch("common.deployment.cli.PrefectProject.process_project_flows")
def test_main_flows_run(mock_process):
    main()
    mock_process.assert_called_with(False)


@patch("sys.argv", ["deploy-cli", "--check-version"])
@patch("common.deployment.cli.cv")
def test_check_version(mock_process):
    main()
    assert mock_process.call_count == 1
