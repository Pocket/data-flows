import os
from pathlib import Path
from unittest.mock import patch

import pytest
from common.deployment.check_version import (
    get_main_version_tag,
    get_poetry_version,
    main,
)


@patch("common.deployment.check_version.run_command")
def test_get_poetry_version(mock_cmd):
    mock_cmd.return_value = "base 0.0.1"
    x = get_poetry_version()
    assert x == ["base", "0.0.1"]
    assert mock_cmd.call_count == 1


@patch("common.deployment.check_version.run_command")
def test_get_main_version_tag(mock_cmd):
    mock_cmd.return_value = "base 0.0.1"
    x = get_main_version_tag()
    assert x == ["base", "0.0.1"]
    assert mock_cmd.call_count == 1


@patch("common.deployment.check_version.get_poetry_version")
@patch("common.deployment.check_version.get_main_version_tag")
def test_main_no_bump(mock_current, mock_new):
    mock_new.return_value = ["base", "0.1.1"]
    mock_current.return_value = ["base", "0.0.1"]
    main()
    assert mock_new.call_count == 1
    assert mock_current.call_count == 1


@patch("common.deployment.check_version.get_poetry_version")
@patch("common.deployment.check_version.get_main_version_tag")
def test_main_no_2_digit_version_new(mock_current, mock_new):
    mock_new.return_value = ["base", "0.1"]
    mock_current.return_value = ["base", "0.0.1"]
    main()
    assert mock_new.call_count == 1
    assert mock_current.call_count == 1


@patch("common.deployment.check_version.get_poetry_version")
@patch("common.deployment.check_version.get_main_version_tag")
def test_main_no_2_digit_version_current(mock_current, mock_new):
    mock_new.return_value = ["base", "0.2.0"]
    mock_current.return_value = ["base", "0.1"]
    main()
    assert mock_new.call_count == 1
    assert mock_current.call_count == 1


@patch("common.deployment.check_version.get_poetry_version")
@patch("common.deployment.check_version.get_main_version_tag")
def test_main_bump_same(mock_current, mock_new):
    mock_new.return_value = ["base", "0.0.1"]
    mock_current.return_value = ["base", "0.0.1"]
    with pytest.raises(Exception):
        main()
    assert mock_new.call_count == 1
    assert mock_current.call_count == 1


@patch("common.deployment.check_version.get_poetry_version")
@patch("common.deployment.check_version.get_main_version_tag")
def test_main_bump_less(mock_current, mock_new):
    mock_new.return_value = ["base", "0.0.1"]
    mock_current.return_value = ["base", "0.0.2"]
    with pytest.raises(Exception):
        main()
    assert mock_new.call_count == 1
    assert mock_current.call_count == 1


@patch("common.deployment.check_version.get_poetry_version")
@patch("common.deployment.check_version.run_command")
def test_main_bump_new(mock_cmd, mock_new):
    mock_new.return_value = ["base", "0.0.1"]
    mock_cmd.side_effect = Exception("Invalid TOML file")
    p = Path("/tmp/common-utils")
    p.mkdir(exist_ok=True)
    with open(os.path.join(p, "pyproject.toml"), "w") as f:
        f.write("404: Not Found")
    main()
    assert mock_new.call_count == 1
    assert mock_cmd.call_count == 1


@patch("common.deployment.check_version.get_poetry_version")
@patch("common.deployment.check_version.run_command")
def test_main_bad_file(mock_cmd, mock_new):
    mock_new.return_value = ["base", "0.0.1"]
    mock_cmd.side_effect = Exception("Invalid TOML file")
    p = Path("/tmp/common-utils")
    p.mkdir(exist_ok=True)
    with open(os.path.join(p, "pyproject.toml"), "w") as f:
        f.write("bad file")
    with pytest.raises(Exception):
        main()
    assert mock_new.call_count == 1
    assert mock_cmd.call_count == 1


@patch("common.deployment.check_version.get_poetry_version")
@patch("common.deployment.check_version.run_command")
def test_main_exception(mock_cmd, mock_new):
    mock_new.return_value = ["base", "0.0.1"]
    mock_cmd.side_effect = Exception("misc")
    with pytest.raises(Exception):
        main()
    assert mock_new.call_count == 1
    assert mock_cmd.call_count == 1
