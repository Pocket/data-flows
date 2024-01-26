import asyncio

import pendulum
import pytest
import requests_mock
from freestar_revenue_reporting.freestar_reporting_flow import (
    FlowDateInputs,
    freestar_report_flow,
)
from prefect import task


@pytest.mark.parametrize("test_config", ["base", "ndrprebid", "force_paging"])
@pytest.mark.parametrize(
    "is_archived",
    [True, False],
)
def test_extract_freestar_data(test_config, is_archived, monkeypatch):
    """Paramatized test to get coverage and validate different scenarios."""

    def create_base_data():
        """
        Creates a return result of 7 days to trigger
        diffing logic.
        """
        start_date = pendulum.now().subtract(days=14)
        end_date = pendulum.now().subtract(days=1)
        dt_period = end_date - start_date
        dt_period.in_days()  # type: ignore
        return [(d.date(),) for d in dt_period]  # type: ignore

    # state dict to make sure paging ends after first page for force_paging
    run_state = {"date_count": 0}

    def create_api_data(request, context):
        """Callback to return config based results with
        logic for handling paging.
        """
        data = test_result_sets[context.headers["test_config"]]["api_results"]
        print(run_state["date_count"])
        if run_state["date_count"] > 1 and test_config == "force_paging":
            return {"data": []}
        run_state["date_count"] += 1
        return data

    result_prefix_mapping = {True: "NdrGcr", False: "NdrPrebid"}

    # config for each run type
    test_result_sets = {
        "base": {
            "api_results": {
                "data": [{f"{result_prefix_mapping[is_archived]}.test": "test"}]
            },
            "sql_results": create_base_data,
            "mock_assertion_count": 19,
            "is_archived": is_archived,
        },
        "ndrprebid": {
            "api_results": {
                "data": [{f"{result_prefix_mapping[is_archived]}.test": "test"}]
            },
            "sql_results": create_base_data,
            "mock_assertion_count": 19,
            "is_archived": is_archived,
        },
        "force_paging": {
            "api_results": {
                "data": [
                    {f"{result_prefix_mapping[is_archived]}.test": 1},
                    {f"{result_prefix_mapping[is_archived]}.test": 2},
                    {f"{result_prefix_mapping[is_archived]}.test": 3},
                ]
            },
            "sql_results": create_base_data,
            "mock_assertion_count": 8,
            "start_date": "2023-12-16",
            "end_date": "2023-12-17",
            "overwrite": True,
            "record_limit": 2,
            "is_archived": is_archived,
        },
    }
    # create input based on config
    dates = FlowDateInputs(
        start_date=test_result_sets[test_config].get("start_date"),
        end_date=test_result_sets[test_config].get("end_date"),
        overwrite=test_result_sets[test_config].get("overwrite", False),
        record_limit=test_result_sets[test_config].get("record_limit", 50000),
        is_archived=is_archived,
    )

    # state for tracking fake task calls
    mock_state = {
        "call_count": 0,
    }

    @task()
    async def fake_query_task(*args, **kwargs):
        """Fake task to return config based results when
        Snowflake query task as called.
        """
        mock_state["call_count"] += 1
        return test_result_sets[test_config]["sql_results"]()

    # patch the Snowflake query tasks
    monkeypatch.setattr(
        "freestar_revenue_reporting.freestar_reporting_flow.snowflake_multiquery",
        fake_query_task,
    )

    monkeypatch.setattr(
        "freestar_revenue_reporting.freestar_reporting_flow.snowflake_query_sync",
        fake_query_task,
    )

    # patch the calls to Request
    # using callback logic mentioned here:
    # https://requests-mock.readthedocs.io/en/latest/response.html#dynamic-response
    with requests_mock.Mocker() as m:
        m.post(
            "https://analytics.pub.network/cubejs-api/v1/load",
            json=create_api_data,
            headers={"test_config": test_config},
        )
        # run flow and assert
        with pytest.raises(Exception):
            asyncio.run(freestar_report_flow(dates=dates))  # type: ignore
            assert (
                mock_state["call_count"]
                == test_result_sets[test_config]["mock_assertion_count"]
            )
            assert (
                mock_state["call_count"]
                == test_result_sets[test_config]["mock_assertion_count"]
            )
            assert (
                mock_state["call_count"]
                == test_result_sets[test_config]["mock_assertion_count"]
            )
