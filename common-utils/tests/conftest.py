import os

import pytest
from common.testing_utils import prefect_test_fixture  # noqa: F401


@pytest.fixture(scope="session", autouse=True)
def tests_setup_and_teardown():
    os.environ.pop("AWS_PROFILE", None)
    yield
