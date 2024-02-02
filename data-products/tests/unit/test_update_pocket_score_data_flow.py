import pytest
from prefect import task
from update_pocket_score_data_flow import update_pocket_score_data


@pytest.mark.asyncio
@pytest.mark.parametrize("with_results", [True, False])
async def test_update_pocket_score_data_flow(with_results, monkeypatch):
    state = {"execute_call_count": 0, "query_call_count": 0}

    @task()
    async def fake_query(*args, **kwargs):
        state["query_call_count"] += 1
        results = [
            (
                "1",
                "2147483646",
                "9",
                "8",
                "7",
                "6",
            ),
            (
                "1",
                "2147483647",
                "9",
                "8",
                "7",
                "6",
            ),
            (
                "2",
                "2147483648",
                "9",
                "8",
                "7",
                "6",
            ),
            (
                "2",
                "2147483645",
                "9",
                "8",
                "7",
                "6",
            ),
            (
                "3",
                "2147483644",
                "9",
                "8",
                "7",
                "6",
            ),
        ]
        if not with_results:
            results = []
        return results

    @task()
    async def fake_execute(*args, **kwargs):
        state["execute_call_count"] += 1
        return 5

    monkeypatch.setattr(
        "update_pocket_score_data_flow.sqlalchemy_execute", fake_execute
    )
    monkeypatch.setattr("update_pocket_score_data_flow.sqlalchemy_query", fake_query)
    await update_pocket_score_data()
    assert state["execute_call_count"] == 2
    assert state["query_call_count"] == 1
