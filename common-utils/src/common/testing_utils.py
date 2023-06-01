from unittest.mock import MagicMock

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


@pytest.fixture()
def mock_snowflake_task(request, monkeypatch):
    """This will allow the mocking of Prefect-Snowflake database
    functions through the use pytest mark decorators along with the
    fixture, for example:

    @pytest.mark.query_results([("test",)])
    @pytest.mark.snowflake_task_name("snowflake_query")
    def test_flow(mock_snowflake_task):
    ...

    See https://docs.pytest.org/en/7.1.x/how-to/fixtures.html#using-markers-to-pass-data-to-fixtures

    Yields:
        MagicMock: This can be used for call assertions.
    """
    result_marker = request.node.get_closest_marker("query_results")
    task_name_marker = request.node.get_closest_marker("snowflake_task_name")
    result_set = result_marker.args[0]
    snowflake_task_name = task_name_marker.args[0]
    mock = MagicMock(return_value=result_set)
    monkeypatch.setattr(f"prefect_snowflake.database.{snowflake_task_name}.fn", mock)
    yield mock


@pytest.fixture()
def reset_script_path(request, monkeypatch):
    """This will allow the mocking of the common.get_script_path helper
    function through the use pytest mark decorators along with the
    fixture, for example:

    SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
    NAMESPACE_OVERRIDE = "snowflake_query_extraction.snowflake_query_extraction_flow"


    @pytest.mark.script_path_override(SCRIPT_PATH)
    @pytest.mark.script_path_namespace(NAMESPACE_OVERRIDE)
    def test_extraction_inputs(reset_script_path):
    ...

    Namespace is needed to support the following import:

    from common import get_script_path

    See https://docs.pytest.org/en/7.1.x/how-to/fixtures.html#using-markers-to-pass-data-to-fixtures
    """
    path_marker = request.node.get_closest_marker("script_path_override")
    namespace_marker = request.node.get_closest_marker("script_path_namespace")
    path_override = path_marker.args[0]
    namespace = namespace_marker.args[0]
    mock = MagicMock(return_value=path_override)
    monkeypatch.setattr(f"{namespace}.get_script_path", mock)
    yield
