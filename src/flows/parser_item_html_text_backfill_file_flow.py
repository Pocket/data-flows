from prefect import Flow, task, unmapped, flatten, Parameter
from prefect.executors import LocalDaskExecutor
from utils.flow import get_flow_name, get_interval_schedule

# Setting flow variables
FLOW_NAME = get_flow_name(__file__)

# This bucket was created by another process. We may have to revisit using this bucket.
S3_BUCKET = 'pocket-snowflake-staging-manual'


with Flow(FLOW_NAME, executor=LocalDaskExecutor(), schedule=get_interval_schedule(minutes=1440)) as flow:
    key = Parameter('key')
    flow.logger.info(f"Processing key: {key}")


if __name__ == "__main__":
    flow.run()
