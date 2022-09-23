import base64
import gzip
import zlib
from datetime import timedelta
from io import BytesIO
from typing import List

import boto3
import pandas as pd
import prefect
from prefect import Flow, task, Parameter, flatten
from prefect.tasks.aws import S3List, S3Download, S3Upload
from prefect.executors import LocalDaskExecutor

from utils import config
from utils.flow import get_flow_name

# Setting flow variables
FLOW_NAME = get_flow_name(__file__)

# This bucket was created by another process. We may have to revisit using this bucket.
SOURCE_S3_BUCKET = 'pocket-snowflake-staging-manual'
SOURCE_PREFIX = 'aurora/textparser-prod-content-snapshot-2022091408-cluster/content/'
STAGE_S3_BUCKET = 'pocket-data-items'
STAGE_S3_PREFIX = 'article/backfill-html-filesplit/'
STAGE_CHUNK_ROWS = 10000
NUM_FILES_PER_RUN = 1000


@task()
def get_source_keys() -> [str]:
    """
    :return: List of S3 keys for the S3_BUCKET and SOURCE_PREFIX
    """
    logger = prefect.context.get("logger")

    keys = S3List().run(bucket=SOURCE_S3_BUCKET, prefix=SOURCE_PREFIX)
    if len(keys) == 0:
        raise Exception(f'No files to process for s3://{SOURCE_S3_BUCKET}/{SOURCE_PREFIX}.')

    if len(keys) > NUM_FILES_PER_RUN:
        logger.warn(f"Number of files is greater than the number a worker can process in a single run. Found {len(keys)} files, processing {NUM_FILES_PER_RUN}.")
        return keys[0:NUM_FILES_PER_RUN]
    else:
        return keys


@task()
def extract(key: str) -> List[pd.DataFrame]:
    logger = prefect.context.get("logger")
    logger.info(f"Extracting file: {str(key)}")
    bucket = SOURCE_S3_BUCKET
    s3 = boto3.resource('s3')
    response = s3.Object(bucket, key).get()
    df_iterator = pd.read_csv(response['Body'], escapechar='\\', sep="|", header=None, usecols=[0, 1], chunksize=100000,
                              names=['resolved_id', 'compressed_html'])
    return [chunk for chunk in df_iterator]


@task()
def transform(df: pd.DataFrame) -> pd.DataFrame:
    df['html'] = [base64.b64encode(zlib.decompress(base64.b64decode(compressed_html))).decode() for compressed_html in
                  df['compressed_html']]
    df.drop(columns=['compressed_html'], inplace=True)
    return df


def stage_chunk(index: int, df: pd.DataFrame) -> str:
    bucket = STAGE_S3_BUCKET
    s3_prefix = STAGE_S3_PREFIX
    key = f"{s3_prefix}{index}.csv.gz"
    csv_buffer = BytesIO()
    with gzip.GzipFile(mode='w', fileobj=csv_buffer) as gz_file:
        df.to_csv(gz_file, index=False)
    s3 = boto3.resource('s3')
    s3.Object(bucket, key).put(Body=csv_buffer.getvalue())
    return key


@task()
def stage(df: pd.DataFrame) -> [str]:
    logger = prefect.context.get("logger")
    chunks = [(i, df[i:i + STAGE_CHUNK_ROWS]) for i in range(0, df.shape[0], STAGE_CHUNK_ROWS)]
    logger.info(f"Number of chunks created: {len(chunks)}.")
    keys = [stage_chunk(i, chunk) for i, chunk in chunks]
    logger.info(f"Staged keys: {*keys,}")
    return keys


with Flow(FLOW_NAME, executor=LocalDaskExecutor(num_workers=config.DASK_WORKERS)) as flow:
    source_keys_results = get_source_keys()
    extract_results = extract.map(source_keys_results)
    transform_results = transform.map(flatten(extract_results))
    stage_results = stage.map(transform_results)


if __name__ == "__main__":
    flow.run()
