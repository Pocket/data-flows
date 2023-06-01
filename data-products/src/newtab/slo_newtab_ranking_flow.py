from datetime import datetime, timedelta
from typing import Dict

from common.databases.snowflake_utils import PktSnowflakeConnector
from common.deployment import FlowSpec, FlowDeployment
from prefect import task, flow
from prefect.blocks.notifications import PagerDutyWebHook
from prefect.server.schemas.schedules import CronSchedule
from prefect_snowflake.database import snowflake_query
from snowflake.connector import DictCursor

from shared.blocks.notifications.pagerduty import get_notification_block

# 0.95 means that the alarm goes off if < 95% of the error budget remains.
ERROR_BUDGET_ALARM_THRESHOLD = 0.95
# Alarm goes off if data points are missing for this duration.
MISSING_DATA_ALARM_THRESHOLD = timedelta(seconds=3600)
SLO_NAME = "NewTab Recommendation Ranking SLO"


METRIC_QUERY = """
select 
    TRUNCATED_RECOMMENDED_AT,
    ERROR_BUDGET
from ANALYTICS.DBT_FACTS.NEWTAB_RECOMMENDATION_RANKING_FRESHNESS_SLO
order by TRUNCATED_RECOMMENDED_AT desc
limit 1
"""


@flow()
async def slo_newtab_ranking_flow():
    metrics = snowflake_query(
        snowflake_connector=PktSnowflakeConnector(),
        query=METRIC_QUERY,
        cursor_type=DictCursor,
    )[0]

    notifier = await get_notification_block(notification_type="non-critical")
    notify_if_error_budget_is_low(metrics, notifier)
    notify_if_data_points_are_missing(metrics, notifier)


@task()
async def notify_if_error_budget_is_low(metrics: Dict, notifier: PagerDutyWebHook):
    error_budget = metrics["ERROR_BUDGET"]

    if error_budget < ERROR_BUDGET_ALARM_THRESHOLD:
        await notifier.notify(
            subject=f"{SLO_NAME} error budget is {error_budget:.1%}",
            body="Runbook: https://getpocket.atlassian.net/l/cp/juY9UDqV",
        )


@task()
async def notify_if_data_points_are_missing(metrics: Dict, notifier: PagerDutyWebHook):
    truncated_recommended_at = metrics["TRUNCATED_RECOMMENDED_AT"]
    data_age = datetime.now() - datetime.fromisoformat(truncated_recommended_at)

    if data_age > MISSING_DATA_ALARM_THRESHOLD:
        await notifier.notify(
            subject=f"{SLO_NAME} is missing data points",
            body=f"Most recent row is {data_age.total_seconds() / 60:.0f} minutes old",
        )


FLOW_SPEC = FlowSpec(
    flow=slo_newtab_ranking_flow,
    docker_env="base",
    deployments=[
        FlowDeployment(
            deployment_name="base",
            # Business hour schedule: At minute 0 from 8am PST through 5pm PST, from Monday through Friday.
            schedule=CronSchedule(
                cron="0 8-17 * * 1-5", timezone="America/Los_Angeles"
            ),
        ),
    ],
)


if __name__ == "__main__":
    slo_newtab_ranking_flow()
