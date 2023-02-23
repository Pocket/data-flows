from common.deployment.cli import parse_args, main
from unittest.mock import patch


def test_parse_args():
    x = parse_args(["--build-only"])
    assert x.build_only
    x = parse_args([])
    assert not x.build_only


@patch("common.deployment.cli.process_project_env")
def test_main(mock_process):
    main([])
    assert mock_process.called
