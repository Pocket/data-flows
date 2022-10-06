import base64
import gzip
import hashlib
import os.path
from io import StringIO, BytesIO

import boto3
import pandas as pd
import prefect
from prefect import Flow, task, Parameter
from prefect.executors import LocalDaskExecutor
from prefect.tasks.aws import S3Download

from common_tasks.transform_data import get_text_from_html
from utils.flow import get_flow_name

# Setting flow variables
FLOW_NAME = get_flow_name(__file__)

# This bucket was created by another process. We may have to revisit using this bucket.
S3_BUCKET = 'pocket-data-items'
STAGE_PREFIX = 'article/backfill-html-filesplit-stage'

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
    contents = S3Download().run(bucket=S3_BUCKET, key=key, compression='gzip', as_bytes=True)
    return pd.read_pickle(BytesIO(contents))


@task()
def transform(df: pd.DataFrame) -> pd.DataFrame:
    df['text'] = [get_text_from_html(html) for html in df['html']]
    df['text_md5'] = [hashlib.md5(t.encode('utf-8')).hexdigest() for t in df['text']]
    return df


def df_to_gip_bytes(df: pd.DataFrame) -> BytesIO:
    csv_buffer = BytesIO()
    with gzip.GzipFile(mode='w', fileobj=csv_buffer) as gz_file:
        df.to_csv(gz_file, index=False)

    return csv_buffer


@task()
def load(df: pd.DataFrame, key: str):
    bucket = S3_BUCKET
    s3_prefix = STAGE_PREFIX
    s3 = boto3.resource('s3')
    logger = prefect.context.get("logger")

    file_name = os.path.basename(key)
    key = f'{s3_prefix}/{file_name}'
    obj = s3.Object(bucket, key)

    logger.info(f"Creating csv and compressing {key}")
    csv_buffer = df_to_gip_bytes(df)

    logger.info(f"Putting {key}")
    obj.put(Body=csv_buffer.getvalue())


with Flow(FLOW_NAME, executor=LocalDaskExecutor(scheduler="threads")) as flow:
    keys = Parameter('keys')
    extract_results = extract.map(keys)
    transform_results = transform.map(extract_results)
    load_results = load.map(transform_results, keys)

if __name__ == "__main__":
    flow.run(parameters=dict(keys=["article/backfill-html-filesplit/2022091408.part_00149_1.csv.gz"]))
