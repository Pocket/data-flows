from unittest.mock import patch
from common.deployment.cli import main, parse_args


def test_parse_args():
    x = parse_args(["--build-only"])
    assert x.build_only
    x = parse_args([])
    assert not x.build_only


@patch("common.deployment.cli.process_project_env")
def test_main(mock_process):
    main([])
    assert mock_process.called


@patch("common.deployment.cli.process_project_env")
def test_main_help(mock_process):
    main()
    assert mock_process.called
