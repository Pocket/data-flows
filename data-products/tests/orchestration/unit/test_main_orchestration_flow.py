from src.orchestration.main_orchestration_flow import (
    main_orchestration_flow,
    orchestrate_flows,
)
import pytest
from prefect.testing.utilities import prefect_test_harness
from unittest.mock import patch


@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        yield


@patch("src.orchestration.main_orchestration_flow.get_run_logger")
def test_orchestrate_flows(mock):
    with prefect_test_harness():
        orchestrate_flows.fn()
        mock.assert_called_once()


@patch("src.orchestration.main_orchestration_flow.orchestrate_flows")
def test_main_orchestration_flow(mock):
    with prefect_test_harness():
        main_orchestration_flow()
        mock.assert_called_once()
