from typing import Literal

from prefect.blocks.notifications import PagerDutyWebHook

from common.settings import CommonSettings


async def get_notification_block(
    notification_type: Literal["critical", "non-critical"]
) -> PagerDutyWebHook:
    cs = CommonSettings()  # type: ignore
    return await PagerDutyWebHook.load(
        f"data-products-pagerduty-{notification_type}-{cs.deployment_type}"
    )
