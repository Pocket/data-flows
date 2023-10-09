import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    """This will apply test fixture to all tests as needed
    when imported.  Best practice will be to import into a conftest.py
    file.
    See https://docs.prefect.io/2.10.11/guides/testing/
    """
    with prefect_test_harness():
        yield