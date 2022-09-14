import json
from io import StringIO
from typing import Union, Tuple, List, Dict

import boto3
import pandas as pd
import prefect
from prefect import Flow, task, unmapped
from prefect.tasks.aws import S3List, S3Download, S3Upload
from prefect.executors import LocalDaskExecutor
from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from common_tasks.transform_data import get_text_from_html

from utils import config
from utils.flow import get_flow_name, get_interval_schedule

'''
'''

# Setting flow variables
FLOW_NAME = get_flow_name(__file__)

S3_BUCKET = 'pocket-data-items'
SOURCE_PREFIX = 'article/streaming-html/'
STAGE_PREFIX = 'article/streaming-html-stage/'
SNOWFLAKE_STAGE='raw.item.article_content_html'
SNOWFLAKE_TABLE='raw.item.article_content_html'
CHUNK_ROWS = 10000

IMPORT_SQL = f"""
    copy into {SNOWFLAKE_TABLE}(event)
    from (
      select $1
      from %(snowflake_stage_uri)s
    )
    file_format = (type = 'JSON')
    on_error=skip_file;
"""
REFRESH_STAGE = f'alter stage {SNOWFLAKE_STAGE} refresh;'

@task()
def get_source_keys() -> [str]:
    file_list = S3List().run(bucket=S3_BUCKET, prefix=SOURCE_PREFIX)
    if len(file_list) == 0:
        raise Exception(f'No files to process for s3://{S3_BUCKET}/{SOURCE_PREFIX}')
    return file_list

@task()
def extract_transform(key: str) -> pd.DataFrame:
    logger = prefect.context.get("logger")
    logger.info(f"Extracting and Transforming file: {str(key)}")
    contents = S3Download().run(bucket=S3_BUCKET, key=key, compression='gzip')
    content_list = [json.loads(l) for l in contents.splitlines()]
    df = pd.DataFrame.from_records(content_list[1:5])
    df.rename(columns={"article": "html"}, inplace=True)
    df['text'] = [get_text_from_html(article_html) for article_html in df['html']]
    return df

def write_df_to_s3(bucket: str, key: str, df: pd.DataFrame) -> str:
    json_buffer = StringIO()
    df.to_json(json_buffer, compression='gzip', orient='records', lines=True)
    s3 = boto3.resource('s3')
    s3.Object(bucket, key).put(Body=json_buffer.getvalue())
    return key

def get_stage_prefix():
    s3_prefix = STAGE_PREFIX
    flow_run_id = prefect.context.get('flow_run_id')
    return f"{s3_prefix}{flow_run_id}"

@task()
def stage(dfs: List[pd.DataFrame]):
    bucket = S3_BUCKET
    s3_prefix = get_stage_prefix()
    df = pd.concat(dfs, sort=False)
    chunks = [(i, df[i:i + CHUNK_ROWS]) for i in range(0, df.shape[0], CHUNK_ROWS)]
    return [write_df_to_s3(bucket, f"{s3_prefix}/{i}.json.gz", df) for i, df in chunks]

@task()
def chunk_transform(dfs: List[pd.DataFrame]):
    if len(dfs) == 0:
        raise Exception(f'No records available in source: s3://{S3_BUCKET}/{SOURCE_PREFIX}')
    df = pd.concat(dfs, sort=False)
    return [(i, df[i:i + CHUNK_ROWS]) for i in range(0, df.shape[0], CHUNK_ROWS)]

@task()
def stage_and_load(chunk: (int, pd.DataFrame)):
    logger = prefect.context.get("logger")
    bucket = S3_BUCKET
    s3_prefix = get_stage_prefix()
    (i, df) = chunk
    key = f"{s3_prefix}/{i}.json.gz"
    write_df_to_s3(bucket, key, df)

    PocketSnowflakeQuery(
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
        output_type = OutputType.DICT
    ).run(query=REFRESH_STAGE)

    path = key[28:] ## Strip prefix "article/streaming-html-stage"
    logger.info(f"Snowflake loading from: {str(key)}")
    PocketSnowflakeQuery(
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
        output_type=OutputType.DICT
    ).run(data={'snowflake_stage_uri': f'@{SNOWFLAKE_STAGE}{path}'}, query=IMPORT_SQL)

    return key

@task()
def combine(source_keys: [str], stage_keys: [str]):
    return source_keys + stage_keys

@task()
def cleanup(bucket: str, key: str):
    logger = prefect.context.get("logger")
    logger.info(f"Cleanup file: {str(key)}")
    s3 = boto3.resource('s3')
    s3.Object(bucket, key).delete()


with Flow(FLOW_NAME, executor=LocalDaskExecutor(), schedule=get_interval_schedule(hours=24)) as flow:
    source_keys_results = get_source_keys()

    transform_results = extract_transform.map(source_keys_results)

    chunk_results = chunk_transform(transform_results)
    stage_key_results = stage_and_load.map(chunk_results)

    combine_keys = combine(source_keys_results, stage_key_results)
    cleanup.map(unmapped(S3_BUCKET), combine_keys)


if __name__ == "__main__":
    flow.run()
