import gzip
import json
from io import StringIO, BytesIO
from typing import Union, Tuple, List, Dict

import boto3
import pandas as pd
import prefect
from prefect import Flow, task, unmapped, flatten
from prefect.tasks.aws import S3List, S3Download, S3Upload
from prefect.executors import LocalDaskExecutor
from prefect.tasks.snowflake import SnowflakeQuery

from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from common_tasks.transform_data import get_text_from_html

from utils import config
from utils.config import SNOWFLAKE_DEFAULT_DICT
from utils.flow import get_flow_name, get_interval_schedule

# Setting flow variables
FLOW_NAME = get_flow_name(__file__)

# This bucket was created by another process. We may have to revisit using this bucket.
S3_BUCKET = 'pocket-data-items'
SOURCE_PREFIX = 'article/streaming-html/'
STAGE_PREFIX = 'article/streaming-html-stage/'
# Maximum number rows to include in a staging file. This is optimized for prefect import performance.
CHUNK_ROWS = 50000  # 3486 rows = 10MB

# Import from S3 to Snowflake
# 3.5k rows = 2 seconds on xsmall warehouse
IMPORT_SQL = f"""
copy into raw.item.article_content_v2
(resolved_id, html, text)
from %(uri)s
storage_integration = aws_integration_readonly_prod
file_format = (type = 'CSV', skip_header=1, FIELD_OPTIONALLY_ENCLOSED_BY='"')
on_error=ABORT_STATEMENT;
"""


@task()
def get_source_keys() -> [str]:
    """
    :return: List of S3 keys for the S3_BUCKET and SOURCE_PREFIX
    """
    file_list = S3List().run(bucket=S3_BUCKET, prefix=SOURCE_PREFIX)
    if len(file_list) == 0:
        raise Exception(
            f'No files to process for s3://{S3_BUCKET}/{SOURCE_PREFIX}. Ensure the firehose delivery stream delivering S3 files is writing objects.')
    return file_list


@task()
def extract(key: str) -> pd.DataFrame:
    """
    - Extracts data from the S3_BUCKET for the {key}
    - Transforms the html content field for each row to text
    - Adds the transformed text data as a new field to the Dataframe
    :return: Transformed Dataframe
    """
    logger = prefect.context.get("logger")
    logger.info(f"Extracting file: {str(key)}")
    contents = S3Download().run(bucket=S3_BUCKET, key=key, compression='gzip')
    dicts = [json.loads(l) for l in contents.splitlines()]
    return pd.DataFrame.from_records(dicts)


@task()
def transform(df: pd.DataFrame) -> pd.DataFrame:
    df.rename(columns={"article": "html"}, inplace=True)
    df['text'] = [get_text_from_html(html) for html in df['html']]
    return df


def get_stage_prefix() -> str:
    """
    The stage prefix is the destination for the transformed stage files used for Snowflake load
    :return: S3 Stage prefix for each run instance.
    """
    s3_prefix = STAGE_PREFIX
    flow_run_id = prefect.context.get('flow_run_id')
    return f"{s3_prefix}{flow_run_id}"


def stage_chunk(index: int, df: pd.DataFrame) -> str:
    bucket = S3_BUCKET
    s3_prefix = get_stage_prefix()
    key = f"{s3_prefix}/{index}.csv.gz"

    csv_buffer = BytesIO()
    with gzip.GzipFile(mode='w', fileobj=csv_buffer) as gz_file:
        df.to_csv(gz_file, index=False)

    s3 = boto3.resource('s3')
    s3.Object(bucket, key).put(Body=csv_buffer.getvalue())
    return key


@task()
def stage(dfs: List[pd.DataFrame]) -> List[str]:
    """
    Stage files in S3 with a file size optimized for import.
    :return: S3 Stage file key.
    """
    logger = prefect.context.get("logger")
    logger.info(f"Staging dataframes count: {len(dfs)}")

    if len(dfs) == 0:
        raise Exception(f'No dataframes to stage.')

    df = pd.concat(dfs, sort=False)
    chunks = [(i, df[i:i + CHUNK_ROWS]) for i in range(0, df.shape[0], CHUNK_ROWS)]
    logger.info(f"Number of chunks created: {len(chunks)}.")
    keys = [stage_chunk(i, chunk) for i, chunk in chunks]
    logger.info(f"Staged keys: {*keys,}")
    return keys


@task()
def load(key: str):
    uri = f"s3://{S3_BUCKET}/{key}"
    logger = prefect.context.get("logger")
    logger.info(f"Snowflake loading key: {uri}")
    return SnowflakeQuery(**SNOWFLAKE_DEFAULT_DICT).run(data={'uri': uri}, query=IMPORT_SQL)


@task()
def cleanup(key: str):
    bucket = S3_BUCKET
    logger = prefect.context.get("logger")
    logger.info(f"deleting file: {str(key)}")
    s3 = boto3.resource('s3')
    s3.Object(bucket, key).delete()


with Flow(FLOW_NAME, executor=LocalDaskExecutor(), schedule=get_interval_schedule(minutes=1440)) as flow:
    source_keys_results = get_source_keys()
    extract_results = extract.map(source_keys_results)
    transform_results = transform.map(extract_results)
    stage_results = stage(transform_results)
    load_results = load.map(stage_results)
    cleanup.map(flatten([source_keys_results, stage_results])).set_upstream(load_results)

if __name__ == "__main__":
    flow.run()
