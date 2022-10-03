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
from prefect.engine.results import LocalResult
from prefect.engine.serializers import PandasSerializer
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

result = LocalResult(serializer=PandasSerializer(file_type="pickle"))

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


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df['html'] = [base64.b64encode(zlib.decompress(base64.b64decode(compressed_html))).decode() for compressed_html in
                  df['compressed_html']]
    df.drop(columns=['compressed_html'], inplace=True)
    return df


def extract_transform(key: str) -> (str, List[pd.DataFrame]):
    logger = prefect.context.get("logger")
    logger.info(f"Extracting file: {str(key)}")
    bucket = SOURCE_S3_BUCKET
    s3 = boto3.resource('s3')
    response = s3.Object(bucket, key).get()
    df_iterator = pd.read_csv(response['Body'], escapechar='\\', sep="|", header=None, usecols=[0, 1], chunksize=100000,
                              names=['resolved_id', 'compressed_html'])
    return (key, [transform(df) for df in df_iterator])


def stage_chunk(key: str, index: int, df: pd.DataFrame) -> str:
    logger = prefect.context.get("logger")
    bucket = STAGE_S3_BUCKET
    s3_prefix = STAGE_S3_PREFIX
    file_prefix = key[key.startswith(SOURCE_PREFIX) and len(SOURCE_PREFIX):]
    stage_key = f"{s3_prefix}{file_prefix}_{index}_{index + STAGE_CHUNK_ROWS - 1}.csv.gz"
    logger.info(f"stage_key: {stage_key}.")
    csv_buffer = BytesIO()
    with gzip.GzipFile(mode='w', fileobj=csv_buffer) as gz_file:
        df.to_csv(gz_file, index=False)
    s3 = boto3.resource('s3')
    s3.Object(bucket, stage_key).put(Body=csv_buffer.getvalue())
    return stage_key


def stage(key_dfs: (str, List[pd.DataFrame])) -> [str]:
    logger = prefect.context.get("logger")
    key, dfs = key_dfs
    logger.info(f"Number of dfs: {len(dfs)}.")
    df = pd.concat(dfs)
    chunks = [(i, df[i:i + STAGE_CHUNK_ROWS]) for i in range(0, df.shape[0], STAGE_CHUNK_ROWS)]
    logger.info(f"Number of chunks created: {len(chunks)}.")
    keys = [stage_chunk(key, i, chunk) for i, chunk in chunks]
    logger.info(f"Staged keys: {*keys,}")
    return keys

@task(result=result)
def split_files_process() -> [str]:
    """
    Split S3 files into smaller stage files
    """
    keys = get_source_keys()
    for key in keys:
        extract_transform_dfs = extract_transform(key)
        stage_results = stage(extract_transform_dfs)
    return keys


with Flow(FLOW_NAME, executor=LocalDaskExecutor(scheduler="threads")) as flow:
    split_files_process()


if __name__ == "__main__":
    flow.run()
