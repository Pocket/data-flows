import pytest
from common.settings import CommonSettings
from prefect.blocks.notifications import PagerDutyWebHook
from shared.blocks.notifications.pagerduty import get_notification_block

CS = CommonSettings() # type: ignore

@pytest.mark.asyncio
async def test_get_notification_block():
        # setup temporary block
        nc_name = f"data-products-pagerduty-non-critical-{CS.deployment_type}"
        c_name = f"data-products-pagerduty-critical-{CS.deployment_type}"
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
