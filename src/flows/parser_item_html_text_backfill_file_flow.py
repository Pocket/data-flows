from datetime import timedelta
import base64
import gzip
import zlib
from io import BytesIO
from typing import List

import boto3
import pandas as pd
import prefect
from prefect import Flow, task, Parameter
from prefect.executors import LocalDaskExecutor
from prefect.tasks.snowflake import SnowflakeQuery
from dask.system import CPU_COUNT

from common_tasks.transform_data import get_text_from_html
from utils import config
from utils.flow import get_flow_name

# Setting flow variables
FLOW_NAME = get_flow_name(__file__)

# This bucket was created by another process. We may have to revisit using this bucket.
SOURCE_S3_BUCKET = 'pocket-snowflake-staging-manual'
STAGE_S3_BUCKET = 'pocket-data-items'
STAGE_S3_PREFIX = 'article/backfill-html-stage/'
STAGE_CHUNK_ROWS = 10000

LOAD_SQL = f"""
copy into snapshot.item.article_content_v2
(resolved_id, html, text)
from %(uri)s
storage_integration = aws_integration_readonly_prod
file_format = (type = 'CSV', skip_header=1, FIELD_OPTIONALLY_ENCLOSED_BY='"')
on_error=ABORT_STATEMENT;
"""


@task(timeout=10 * 60, max_retries=18, retry_delay=timedelta(seconds=10))
def extract(key: str) -> List[pd.DataFrame]:
    logger = prefect.context.get("logger")
    logger.info(f"Extracting file: {str(key)}")
    bucket = SOURCE_S3_BUCKET
    s3 = boto3.resource('s3')
    response = s3.Object(bucket, key).get()
    df_iterator = pd.read_csv(response['Body'], escapechar='\\', sep="|", header=None, usecols=[0, 1], chunksize=100000,
                              names=['resolved_id', 'compressed_html'])
    return [chunk for chunk in df_iterator]


@task(timeout=10 * 60, max_retries=18, retry_delay=timedelta(seconds=10))
def transform(df: pd.DataFrame) -> pd.DataFrame:
    df['html'] = [zlib.decompress(base64.b64decode(compressed_html)).decode() for compressed_html in
                  df['compressed_html']]
    df.drop(columns=['compressed_html'], inplace=True)
    df['text'] = [get_text_from_html(html) for html in df['html']]
    return df


def get_stage_prefix() -> str:
    """
    The stage prefix is the destination for the transformed stage files used for Snowflake load
    :return: S3 Stage prefix for each run instance.
    """
    s3_prefix = STAGE_S3_PREFIX
    flow_run_id = prefect.context.get('flow_run_id')
    return f"{s3_prefix}{flow_run_id}"


def stage_chunk(index: int, df: pd.DataFrame) -> str:
    bucket = STAGE_S3_BUCKET
    s3_prefix = get_stage_prefix()
    key = f"{s3_prefix}/{index}.csv.gz"

    csv_buffer = BytesIO()
    with gzip.GzipFile(mode='w', fileobj=csv_buffer) as gz_file:
        df.to_csv(gz_file, index=False)

    s3 = boto3.resource('s3')
    s3.Object(bucket, key).put(Body=csv_buffer.getvalue())
    return key


@task(timeout=10 * 60, max_retries=18, retry_delay=timedelta(seconds=10))
def stage(dfs: List[pd.DataFrame]) -> [str]:
    logger = prefect.context.get("logger")
    df = pd.concat(dfs)
    chunks = [(i, df[i:i + STAGE_CHUNK_ROWS]) for i in range(0, df.shape[0], STAGE_CHUNK_ROWS)]
    logger.info(f"Number of chunks created: {len(chunks)}.")
    keys = [stage_chunk(i, chunk) for i, chunk in chunks]
    logger.info(f"Staged keys: {*keys,}")
    return keys


@task(timeout=10 * 60, max_retries=18, retry_delay=timedelta(seconds=10))
def load(key: str) -> str:
    uri = f"s3://{STAGE_S3_BUCKET}/{key}"
    logger = prefect.context.get("logger")
    logger.info(f"Snowflake loading key: {uri}")
    SnowflakeQuery(**config.SNOWFLAKE_DEFAULT_DICT).run(data={'uri': uri}, query=LOAD_SQL)
    return key


@task(timeout=10 * 60, max_retries=18, retry_delay=timedelta(seconds=10))
def cleanup(key: str):
    bucket = STAGE_S3_BUCKET
    logger = prefect.context.get("logger")
    logger.info(f"deleting file: {str(key)}")
    s3 = boto3.resource('s3')
    s3.Object(bucket, key).delete()


with Flow(FLOW_NAME, executor=LocalDaskExecutor(num_workers=config.DASK_WORKERS)) as flow:
    key = Parameter('key', required=True)
    flow.logger.info(f'Processing with {CPU_COUNT} CPUs')

    extract_result = extract(key)
    transform_result = transform.map(extract_result)
    stage_result = stage(transform_result)
    load_result = load.map(stage_result)
    cleanup.map(load_result)

if __name__ == "__main__":
    flow.run(
        parameters={'key': 'aurora/textparser-prod-content-snapshot-2022091408-cluster/content-peek/small.part_00000'})
