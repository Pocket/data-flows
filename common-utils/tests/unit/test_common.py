import os

from common import find_pyproject_file, get_script_path

SCRIPT_PATH_VALUE = os.path.dirname(os.path.realpath(__file__))


def test_get_script_path():
    x = get_script_path()
    assert x == SCRIPT_PATH_VALUE


def test_find_pyproject_file():
    x = find_pyproject_file(get_script_path())
    assert x == "/Users/mozilla/projects/data-flows/common-utils/pyproject.toml"
