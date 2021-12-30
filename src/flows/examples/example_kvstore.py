from datetime import datetime, timedelta
import prefect
from prefect import task, Flow
from prefect.backend import set_key_value, get_key_value

LAST_EXECUTED_KEY = 'example-kvstore-last-executed'

@task
def get_last_execution_date():
    last_executed = get_key_value(LAST_EXECUTED_KEY)
    return datetime.strptime(last_executed, "%Y-%m-%d")

@task
def run_etl(start_date):
    logger = prefect.context.get("logger")
    while start_date <= datetime.today():
        logger.info(f"Running ETL for date {start_date.strftime('%Y-%m-%d')}")
        # do some etl
        start_date += timedelta(days=1)
    return start_date.strftime('%Y-%m-%d')

@task
def set_last_execution_date(date):
    set_key_value(key=LAST_EXECUTED_KEY, value=date)

with Flow('example-kvstore') as flow:
    last_executed_date = get_last_execution_date()
    final_execution_date = run_etl(last_executed_date)
    set_last_execution_date(final_execution_date)

flow.run()
