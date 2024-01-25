from datetime import datetime

import pandas as pd
import pytest
from prefect import flow, task
from recommendation_api.user_impressions_flow import BASE_QUERY, CS, user_impressions


@pytest.mark.asyncio
async def test_user_impressions(monkeypatch):
    state = {"sf_call_count": 0, "load_call_count": 0}

    @task()
    async def fake_task(*args, **kwargs):
        state["sf_call_count"] += 1
        assert kwargs["query"] == BASE_QUERY
        return pd.DataFrame().from_dict([{"UPDATED_AT": datetime.now()}])  # type: ignore

    @flow()
    async def fake_flow(*args, **kwargs):
        environment_map = {"dev": "development", "production": "production"}

        state["load_call_count"] += 1
        assert (
            kwargs["feature_group_name"]
            == f"{environment_map[CS.dev_or_production]}-user-impressions-v2"
        )
        return True

    monkeypatch.setattr(
        "recommendation_api.user_impressions_flow.query_to_dataframe", fake_task
    )

    monkeypatch.setattr(
        "recommendation_api.user_impressions_flow.dataframe_to_feature_group",
        fake_flow,
    )

    await user_impressions()  # type: ignore
    assert state["sf_call_count"] == 1
    assert state["load_call_count"] == 1
