import pytest
from prefect import flow, task
from recommendation_api.corpus_item_engagement_flow import (
    BASE_QUERY,
    CS,
    corpus_item_engagement,
)


@pytest.mark.asyncio
async def test_corpus_item_engagement(monkeypatch):
    state = {"sf_call_count": 0, "load_call_count": 0}

    @task()
    async def fake_task(*args, **kwargs):
        state["sf_call_count"] += 1
        assert kwargs["query"] == BASE_QUERY
        return []

    @flow()
    async def fake_flow(*args, **kwargs):
        environment_map = {"dev": "development", "production": "production"}

        state["load_call_count"] += 1
        assert (
            kwargs["feature_group_name"]
            == f"{environment_map[CS.dev_or_production]}-corpus-engagement-v1"
        )
        return True

    monkeypatch.setattr(
        "recommendation_api.corpus_item_engagement_flow.query_to_dataframe", fake_task
    )

    monkeypatch.setattr(
        "recommendation_api.corpus_item_engagement_flow.dataframe_to_feature_group",
        fake_flow,
    )

    await corpus_item_engagement()  # type: ignore
    assert state["sf_call_count"] == 1
    assert state["load_call_count"] == 1
