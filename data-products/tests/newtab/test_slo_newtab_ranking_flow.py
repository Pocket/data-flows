from datetime import datetime, timedelta
from typing import Optional

import pytest
from prefect.blocks.abstract import NotificationBlock

from newtab.slo_newtab_ranking_flow import (
    notify_if_error_budget_is_low,
    notify_if_data_points_are_missing,
)


class DummyNotifier(NotificationBlock):
    def __init__(self):
        self.notifications = []

    async def notify(self, body: str, subject: Optional[str] = None) -> None:
        self.notifications.append((subject, body))


@pytest.fixture()
def notifier_fixture():
    return DummyNotifier()


@pytest.mark.asyncio
async def test_error_budget_high(notifier_fixture):
    await notify_if_error_budget_is_low.fn({"ERROR_BUDGET": 0.99}, notifier_fixture)
    assert len(notifier_fixture.notifications) == 0


@pytest.mark.asyncio
async def test_error_budget_low(notifier_fixture):
    await notify_if_error_budget_is_low.fn({"ERROR_BUDGET": 0.6789}, notifier_fixture)

    assert len(notifier_fixture.notifications) == 1
    subject, body = notifier_fixture.notifications[0]
    assert "error budget is 67.9%" in subject


@pytest.mark.asyncio
async def test_data_points_not_missing(notifier_fixture):
    metrics = {
        "TRUNCATED_RECOMMENDED_AT": (datetime.now() - timedelta(minutes=50)).isoformat()
    }
    await notify_if_data_points_are_missing.fn(metrics, notifier_fixture)

    assert len(notifier_fixture.notifications) == 0


@pytest.mark.asyncio
async def test_data_points_missing(notifier_fixture):
    metrics = {
        "TRUNCATED_RECOMMENDED_AT": (datetime.now() - timedelta(minutes=70)).isoformat()
    }
    await notify_if_data_points_are_missing.fn(metrics, notifier_fixture)

    assert len(notifier_fixture.notifications) == 1
    subject, body = notifier_fixture.notifications[0]
    assert "missing data points" in subject
    assert body == "Most recent row is 70 minutes old"
