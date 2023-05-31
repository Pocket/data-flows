import pytest
from prefect.blocks.notifications import PagerDutyWebHook
from prefect.testing.utilities import prefect_test_harness

from shared.blocks.notifications.pagerduty import get_notification_block


@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        yield


@pytest.mark.asyncio
async def test_get_notification_block():
    with prefect_test_harness():
        # setup temporary block
        nc_name = "data-products-pagerduty-non-critical-dev"
        c_name = "data-products-pagerduty-critical-dev"
        api_key = "test"
        integration_key = "test"
        await PagerDutyWebHook(
            api_key=api_key, integration_key=integration_key  # type: ignore
        ).save(nc_name, overwrite=True)
        await PagerDutyWebHook(
            api_key=api_key, integration_key=integration_key  # type: ignore
        ).save(c_name, overwrite=True)
        # test
        x = await get_notification_block("non-critical")
        y = await get_notification_block("critical")

        assert isinstance(x, PagerDutyWebHook)
        assert isinstance(y, PagerDutyWebHook)
        assert x._block_document_name == nc_name
        assert y._block_document_name == c_name
