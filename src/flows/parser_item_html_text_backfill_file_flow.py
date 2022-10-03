import base64
import gzip
import hashlib
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
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import ECSRun
from prefect.tasks.snowflake import SnowflakeQuery

from common_tasks.transform_data import get_text_from_html
from utils import config
from utils.flow import get_flow_name

# Setting flow variables
FLOW_NAME = get_flow_name(__file__)

# This bucket was created by another process. We may have to revisit using this bucket.
SOURCE_S3_BUCKET = 'pocket-data-items'
SOURCE_S3_PREFIX = 'article/backfill-html-filesplit/'
STAGE_S3_BUCKET = 'pocket-data-items'
STAGE_S3_PREFIX = 'article/backfill-html-stage/'
STAGE_CHUNK_ROWS = 10000

LOAD_SQL = f"""
copy into snapshot.item.article_content_v2
(resolved_id, html, text, text_md5)
from %(uri)s
storage_integration = aws_integration_readonly_prod
file_format = (type = 'CSV', skip_header=1, FIELD_OPTIONALLY_ENCLOSED_BY='"')
on_error=ABORT_STATEMENT;
"""

result = LocalResult(serializer=PandasSerializer(file_type="pickle"))

@task(result=result)
def extract(key: str) -> List[pd.DataFrame]:
    logger = prefect.context.get("logger")
    logger.info(f"Extracting file: {str(key)}")
    bucket = SOURCE_S3_BUCKET
    s3 = boto3.resource('s3')
    response = s3.Object(bucket, key).get()
    df_iterator = pd.read_csv(response['Body'], compression='gzip', chunksize=100000)
    return [chunk for chunk in df_iterator]


@task(result=result)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    df['html'] = [base64.b64decode(html).decode() for html in df['html']]
    df['text'] = [get_text_from_html(html) for html in df['html']]
    df['text_md5'] = [hashlib.md5(t.encode('utf-8')).hexdigest() for t in df['text']]
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


@task()
def stage(dfs: List[pd.DataFrame]) -> [str]:
    logger = prefect.context.get("logger")
    df = pd.concat(dfs)
    chunks = [(i, df[i:i + STAGE_CHUNK_ROWS]) for i in range(0, df.shape[0], STAGE_CHUNK_ROWS)]
    logger.info(f"Number of chunks created: {len(chunks)}.")
    keys = [stage_chunk(i, chunk) for i, chunk in chunks]
    logger.info(f"Staged keys: {*keys,}")
    return keys


@task()
def load(key: str) -> str:
    uri = f"s3://{STAGE_S3_BUCKET}/{key}"
    logger = prefect.context.get("logger")
    logger.info(f"Snowflake loading key: {uri}")
    SnowflakeQuery(**config.SNOWFLAKE_DEFAULT_DICT).run(data={'uri': uri}, query=LOAD_SQL)
    return key


@task()
def cleanup(key: str):
    bucket = STAGE_S3_BUCKET
    logger = prefect.context.get("logger")
    logger.info(f"deleting file: {str(key)}")
    s3 = boto3.resource('s3')
    s3.Object(bucket, key).delete()


with Flow(FLOW_NAME, executor=LocalDaskExecutor(scheduler="threads")) as flow:
    key = Parameter('key', required=True)
    extract_result = extract(key)
    transform_result = transform.map(extract_result)
    stage_result = stage(transform_result)
    load_result = load.map(stage_result)
    cleanup.map(flatten([load_result, [key]]))

if config.ENVIRONMENT == 'production':
    flow.run_config = ECSRun(cpu='16 vcpu', memory='32 GB')

if __name__ == "__main__":
    flow.run(
        parameters={'key': f'{SOURCE_S3_PREFIX}0.csv.gz'})
