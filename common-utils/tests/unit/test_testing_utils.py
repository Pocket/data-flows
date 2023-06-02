import os

import pytest
from prefect import flow, get_run_logger

from common import get_script_path
from common.testing_utils import reset_script_path  # noqa: F401

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))


@pytest.mark.script_path_override(SCRIPT_PATH)
@pytest.mark.script_path_namespace("common")
def test_all(reset_script_path):  # noqa: F811
    assert get_script_path() == SCRIPT_PATH

    @flow()
    def test_flow():
        logger = get_run_logger()
        logger.info("test")

    test_flow()
