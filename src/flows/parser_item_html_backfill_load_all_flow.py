import math

import numpy as np
import prefect
from prefect import Flow, task, Parameter
from prefect.tasks.aws import S3List
from prefect.tasks.prefect import create_flow_run

from utils import config
from utils.flow import get_flow_name
from flows.parser_item_html_backfill_load_file_flow import FLOW_NAME as file_flow_name

# Setting flow variables
FLOW_NAME = get_flow_name(__file__)
S3_BUCKET = 'pocket-data-items'
SOURCE_PREFIX = 'article/backfill-html-filesplit/'
NUM_FILES_PER_RUN = 1000
NUM_WORKERS = 5


@task()
def get_source_keys(num_files: int) -> [str]:
    """
    :return: List of S3 keys for the S3_BUCKET and SOURCE_PREFIX
    """
    logger = prefect.context.get("logger")

    file_list = S3List().run(bucket=S3_BUCKET, prefix=SOURCE_PREFIX)
    if len(file_list) == 0:
        raise Exception(f'No files to process for s3://{S3_BUCKET}/{SOURCE_PREFIX}')

    if len(file_list) > num_files:
        logger.warn(
            f"Number of files is greater than the number a worker can process in a single run. Found {len(file_list)} files, processing {num_files}.")

    return file_list[:num_files]


@task()
def process(keys, num_workers: int):
    logger = prefect.context.get("logger")
    for keys in np.array_split(keys, num_workers):
        logger.info(f"Queuing worker for keys: {*keys,}.")
        create_flow_run.run(flow_name=file_flow_name, project_name=config.PREFECT_PROJECT_NAME,
                            parameters={"keys": keys.tolist()})


with Flow(FLOW_NAME) as flow:
    num_workers = Parameter('num_workers', default=NUM_WORKERS)
    num_files = Parameter('num_files', default=NUM_FILES_PER_RUN)
    source_keys = get_source_keys(num_files)
    process(source_keys, num_workers)

if __name__ == "__main__":
    flow.run()
