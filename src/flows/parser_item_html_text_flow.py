import json
import re
from datetime import timedelta
from io import StringIO
from typing import Union, Tuple, List, Dict

import boto3
import html2text as html2text
import pandas as pd
import prefect
from prefect import Flow, task, flatten
from prefect.tasks.aws import S3List, S3Download, S3Upload

from utils.flow import get_flow_name, get_interval_schedule

'''
'''

# Setting flow variables
FLOW_NAME = get_flow_name(__file__)
SOURCE_BUCKET = 'pocket-data-items'
SOURCE_PREFIX = 'article/streaming-html/'
STAGE_BUCKET = SOURCE_BUCKET
STAGE_PREFIX = 'article/streaming-html-stage/frame'


@task()
def get_source_keys() -> [str]:
    return S3List().run(bucket=SOURCE_BUCKET, prefix=SOURCE_PREFIX)


@task()
def extract(key: str) -> List[Dict[str, any]]:
    contents = S3Download().run(bucket='foo', key=key, compression='gzip')
    return [json.loads(l) for l in contents.splitlines()]


def get_text_from_html(html):
    h = html2text.HTML2Text()
    h.ignore_links = True
    h.ignore_images = True
    h.ignore_emphasis = True
    h.body_width = 1024
    h.strong_mark = ''
    h.emphasis_mark = ''
    h.ul_item_mark = ''
    # Prevent markdown characters from being escaped.
    re_dummy = re.compile('(.^)(.^)')
    html2text.config.RE_MD_BACKSLASH_MATCHER = re_dummy
    html2text.config.RE_MD_DOT_MATCHER = re_dummy
    html2text.config.RE_MD_PLUS_MATCHER = re_dummy
    html2text.config.RE_MD_DASH_MATCHER = re_dummy

    text = h.handle(str(html))

    # Remove heading markdown
    text = re.sub(r"#+\s+", "", text)
    # Remove table markdown (ignore_tables doesn't add spacing inbetween columns)
    text = re.sub(r"-+\|[\-\|]+ *\n", "", text)
    text = re.sub(r"\| ", "\n", text)
    # Remove horizontal rules
    text = re.sub(r"\* \* \*\n\n", "", text)
    # Remove trailing whitespace
    text = re.sub(r"\s*?(\n{1,2})\s*", r"\1", text)
    text = text.strip()

    return text


@task()
def transform(data: List[Dict[str, any]]) -> pd.dataframe:
    df = pd.DataFrame.from_records(data)
    df.rename(columns={"article": "html"})
    df['text'] = [get_text_from_html(html) for html in df['html']]
    return df


def write_df_to_s3(bucket: str, key: str, df: pd.dataframe) -> str:
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, compression='gzip')
    s3 = boto3.resource('s3')
    s3.Object(bucket, key).put(Body=csv_buffer.getvalue())
    return key


def get_stage_prefix():
    s3_prefix = SOURCE_PREFIX
    flow_run_id = prefect.context.get('flow_run_id')

    return f"{s3_prefix}/{flow_run_id}"


@task()
def stage(dfs: List[pd.dataframe]):
    chunk_rows = 10000
    bucket = SOURCE_BUCKET
    s3_prefix = get_stage_prefix()
    df = pd.concat(dfs, sort=False)
    chunks = [df[i:i + chunk_rows] for i in range(0, df.shape[0], chunk_rows)]
    return [write_df_to_s3(bucket, f"{s3_prefix}/{i}.csv.gz", df) for i, df in chunks]


#TODO Fix SQL and stage all files to a unique prefext so that we can load all files with one command.
IMPORT_SQL = """
COPY INTO stream.item.article (
      resolved_id,
      html,
      text,
      s3_filename_splitpath, 
      s3_file_row_number
    )
FROM (
    SELECT
        %(batch_id)s,
        to_timestamp($1:submission_timestamp::integer/1000000) as submission_timestamp,
        split(metadata$filename,'/'),
        metadata$file_row_number
    FROM %(snowflake_stage_uri)s
)
FILE_FORMAT = (type = 'CSV')
ON_ERROR=ABORT_STATEMENT
"""


@task()
def load(keys: [str]):
    s3_prefix = get_stage_prefix()
    # TODO: calculate snowflake stage uri for task_run_id, Run prefect import here with IMPORT_SQL
    return []  # s3 keys


@task()
def cleanup(bucket: str, keys: [str]):
    s3 = boto3.resource('s3')
    # TODO Can we do many delete commands at one and await the result?
    for key in keys:
        s3.Object(bucket, key).delete()


with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=60)) as flow:
    source_keys_results = get_source_keys()
    extract_results = extract.map(source_keys_results)
    transform_results = transform.map(extract_results)
    stage_results = stage(transform_results)
    load_results = load(stage_results)
    cleanup(SOURCE_BUCKET, source_keys_results, upstream_tasks=[load_results])
    cleanup(STAGE_BUCKET, stage_results, upstream_tasks=[load_results])


if __name__ == "__main__":
    flow.run()
