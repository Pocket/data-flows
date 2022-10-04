import base64
import gzip
import hashlib
from io import StringIO, BytesIO
from typing import List

import boto3
import pandas as pd
import prefect
from prefect import Flow, task, flatten, Parameter
from prefect.tasks.aws import S3Download
from prefect.tasks.snowflake import SnowflakeQuery

from common_tasks.transform_data import get_text_from_html
from utils.config import SNOWFLAKE_DEFAULT_DICT
from utils.flow import get_flow_name

# Setting flow variables
FLOW_NAME = get_flow_name(__file__)

# This bucket was created by another process. We may have to revisit using this bucket.
S3_BUCKET = 'pocket-data-items'
STAGE_PREFIX = 'article/backfill-html-filesplit-stage/'
# Maximum number rows to include in a staging file. This is optimized for prefect import performance.
CHUNK_ROWS = 50000  # 3486 rows = 10MB

# Import from S3 to Snowflake
# 3.5k rows = 2 seconds on xsmall warehouse
IMPORT_SQL = f"""
copy into raw.item.article_content_v2
(resolved_id, html, text, text_md5)
from %(uri)s
storage_integration = aws_integration_readonly_prod
file_format = (type = 'CSV', skip_header=1, FIELD_OPTIONALLY_ENCLOSED_BY='"')
on_error=ABORT_STATEMENT;
"""



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
    df = pd.read_csv(StringIO(contents))
    df['html'] = [base64.b64decode(html) for html in df['html']]
    return df


@task()
def transform(df: pd.DataFrame) -> pd.DataFrame:
    df['text'] = [get_text_from_html(html) for html in df['html']]
    df['text_md5'] = [hashlib.md5(t.encode('utf-8')).hexdigest() for t in df['text']]
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
def stage(df: pd.DataFrame) -> List[str]:
    """
    Stage files in S3 with a file size optimized for import.
    :return: S3 Stage file key.
    """
    logger = prefect.context.get("logger")
    logger.info(f"Staging dataframes")
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


with Flow(FLOW_NAME) as flow:
    key = Parameter('key')
    extract_results = extract(key)
    transform_results = transform(extract_results)
    stage_results = stage(transform_results)
    load_results = load.map(stage_results)
    cleanup.map(key).set_upstream(load_results)
    cleanup.map(stage_results).set_upstream(load_results)


if __name__ == "__main__":
    flow.run(parameters=dict(key="article/backfill-html-filesplit/2022091408.part_00149_0.csv.gz"))
