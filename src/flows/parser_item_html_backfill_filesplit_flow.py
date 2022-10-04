import base64
import gzip
import zlib
from io import BytesIO

import boto3
import pandas as pd
import prefect
from prefect import Flow, task, Parameter, unmapped
from prefect.executors import LocalDaskExecutor

from utils.flow import get_flow_name

# Setting flow variables
FLOW_NAME = get_flow_name(__file__)

# This bucket was created by another process. We may have to revisit using this bucket.
SOURCE_S3_BUCKET = 'pocket-snowflake-staging-manual'
SOURCE_S3_PREFIX = 'aurora/textparser-prod-content-snapshot-2022091408-cluster/content/'
STAGE_S3_BUCKET = 'pocket-data-items'
STAGE_S3_PREFIX = 'article/backfill-html-filesplit/'
STAGE_CHUNK_ROWS = 10000
NUM_FILES_PER_RUN = 1000


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df['html'] = [base64.b64encode(zlib.decompress(base64.b64decode(compressed_html))).decode() for compressed_html in
                  df['compressed_html']]
    df.drop(columns=['compressed_html'], inplace=True)
    return df

@task()
def split_file(key: str) -> [(int, pd.DataFrame)]:
    logger = prefect.context.get("logger")
    logger.info(f"Extracting file: {str(key)}")
    bucket = SOURCE_S3_BUCKET
    s3 = boto3.resource('s3')
    response = s3.Object(bucket, key).get()
    df_iterator = pd.read_csv(
        response['Body'], escapechar='\\', sep="|", header=None, usecols=[0, 1], chunksize=STAGE_CHUNK_ROWS,
        names=['resolved_id', 'compressed_html'])
    return list(enumerate(df_iterator))


def stage_chunk(key: str, index: int, df: pd.DataFrame) -> str:
    logger = prefect.context.get("logger")
    bucket = STAGE_S3_BUCKET
    s3_prefix = STAGE_S3_PREFIX
    file_prefix = key[key.startswith(SOURCE_S3_PREFIX) and len(SOURCE_S3_PREFIX):]
    stage_key = f"{s3_prefix}{file_prefix}_{index}.csv.gz"
    logger.info(f"stage_key: {stage_key}.")
    csv_buffer = BytesIO()
    with gzip.GzipFile(mode='w', fileobj=csv_buffer) as gz_file:
        df.to_csv(gz_file, index=False)
    s3 = boto3.resource('s3')
    s3.Object(bucket, stage_key).put(Body=csv_buffer.getvalue())
    return stage_key


@task()
def transform_stage_process(df_chunk: (int, pd.DataFrame), key: str):
    """
    Split S3 files into smaller stage files
    """
    index, df = df_chunk
    df_transformed = transform(df)
    stage_chunk(key=key, index=index, df=df_transformed)


with Flow(FLOW_NAME, executor=LocalDaskExecutor(scheduler="processes", num_workers=50)) as flow:
    key = Parameter('key', required=True)
    df_chunks = split_file(key)
    transform_stage_process.map(df_chunks, key=unmapped(key))


if __name__ == "__main__":
    flow.run(
        parameters={'key': f'{SOURCE_S3_PREFIX}2022091408.part_00000'})
