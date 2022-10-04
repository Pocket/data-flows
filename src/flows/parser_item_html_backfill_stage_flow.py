import prefect
from prefect import Flow, task, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.tasks.aws import S3List
from prefect.tasks.prefect import create_flow_run

from flows import parser_item_html_backfill_filesplit_flow as file_flow
from utils import config
from utils.flow import get_flow_name

# Setting flow variables
FLOW_NAME = get_flow_name(__file__)

# This bucket was created by another process. We may have to revisit using this bucket.
S3_BUCKET = file_flow.SOURCE_S3_BUCKET
SOURCE_PREFIX = file_flow.SOURCE_S3_PREFIX
NUM_FILES_PER_RUN = 1000

@task()
def get_source_keys() -> [str]:
    """
    :return: List of S3 keys for the S3_BUCKET and SOURCE_PREFIX
    """
    logger = prefect.context.get("logger")

    keys = S3List().run(bucket=S3_BUCKET, prefix=SOURCE_PREFIX)
    if len(keys) == 0:
        raise Exception(f'No files to process for s3://{S3_BUCKET}/{SOURCE_PREFIX}.')

    if len(keys) > NUM_FILES_PER_RUN:
        logger.warn(f"Number of files is greater than the number a worker can process in a single run. Found {len(keys)} files, processing {NUM_FILES_PER_RUN}.")
        return keys[0:NUM_FILES_PER_RUN]
    else:
        return keys

@task()
def map_keys(keys: [str]):
    return [{"key": key} for key in keys]

with Flow(FLOW_NAME, executor=LocalDaskExecutor()) as flow:
    source_keys_results = get_source_keys()
    map_keys_results = map_keys(source_keys_results)

    create_flow_run.map(flow_name=unmapped(file_flow.FLOW_NAME),
                    project_name=unmapped(config.PREFECT_PROJECT_NAME),
                    parameters=map_keys_results)


if __name__ == "__main__":
    flow.run()
