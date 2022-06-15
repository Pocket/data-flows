import re
from datetime import timedelta

from prefect.engine.results import S3Result
from prefect.schedules import IntervalSchedule, Schedule
from prefect.schedules.clocks import CronClock

from utils.config import PREFECT_S3_RESULT_BUCKET, ENVIRONMENT, ENV_PROD


def get_flow_name(file_path: str) -> str:
    """
    :param file_path: Path of the flow file
    :return: Name of the flow, which is the part of the path after /flows/, without the .py extension.
    """
    return re.search(r'/flows/(?P<flow_name>.*)\.py', file_path).group('flow_name')


def get_s3_result() -> S3Result:
    """
    :return: Prefect S3Result that can be set on the flow, which allow us to resume flows from failure using data in S3.
    """
    return S3Result(bucket=PREFECT_S3_RESULT_BUCKET)


def get_interval_schedule(minutes: int) -> IntervalSchedule:
    """
    :return: Prefect IntervalSchedule for PROD only environment.
    """
    if ENVIRONMENT == ENV_PROD:
        schedule = IntervalSchedule(interval=timedelta(minutes=minutes))
    else:
        schedule = None

    return schedule

def get_cron_schedule(cron: str) -> Schedule:
    """
    :return: Prefect Schedule based on CronClock for PROD only environment.
    """
    if ENVIRONMENT == ENV_PROD:
        schedule = Schedule(clocks=[CronClock(cron)])
    else:
        schedule = None

    return schedule
