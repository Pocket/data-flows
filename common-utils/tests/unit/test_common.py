from common import get_script_path
import os

SCRIPT_PATH_VALUE = os.path.dirname(os.path.realpath(__file__))

def test_get_script_path():
    x = get_script_path()
    assert x == SCRIPT_PATH_VALUE