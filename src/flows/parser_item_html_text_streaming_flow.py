import gzip
import hashlib
import json
import os
import pathlib
from copy import deepcopy
from io import BytesIO
from typing import List

import boto3
import pandas as pd
import pendulum
import prefect
from prefect import Flow, flatten, task
from prefect.executors import LocalDaskExecutor
from prefect.tasks.aws import S3Download, S3List
from prefect.tasks.snowflake import SnowflakeQueriesFromFile, SnowflakeQuery

from common_tasks.transform_data import get_text_from_html
from utils.config import ARTICLES_DB_SCHEMA_DICT, ENVIRONMENT, SNOWFLAKE_DEFAULT_DICT
from utils.flow import get_flow_name, get_interval_schedule

# Setting flow variables
FLOW_NAME = get_flow_name(__file__)

# This bucket was created by another process. We may have to revisit using this bucket.
S3_BUCKET = "pocket-data-items"
SOURCE_PREFIX = "article/streaming-html/"
STAGE_PREFIX = "article/streaming-html-stage/"
# Maximum number rows to include in a staging file. This is optimized for prefect import performance.
CHUNK_ROWS = 50000  # 3486 rows = 10MB
NUM_FILES_PER_RUN = 1000

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

# get flow db and schema names from config for ordering tasks
DATABASE_NAME = ARTICLES_DB_SCHEMA_DICT[ENVIRONMENT]["articles_database_name"]
SCHEMA_NAME = ARTICLES_DB_SCHEMA_DICT[ENVIRONMENT]["articles_schema_name"]

# create copy of default dict and add flow db and schema names for ordering tasks
FLOW_SNOWFLAKE_DICT = deepcopy(SNOWFLAKE_DEFAULT_DICT)
FLOW_SNOWFLAKE_DICT["database"] = DATABASE_NAME
FLOW_SNOWFLAKE_DICT["schema"] = SCHEMA_NAME

# Need to check the last_ordered value to determine if reorder is needed
WEEKLY_DEDUP_CHECK_SQL = """select datediff('hours', max(last_ordered), current_timestamp()) > 168 as ready_for_reordering
   from IDENTIFIER(%(full_name)s);"""

# create a base sql file path that will work locally and in prod docker env
BASE_SQL_FILE_PATH = os.path.join(
    os.getenv("DATA_FLOWS_SOURCE_DIR", pathlib.Path(__file__).parents[1]),
    "flows/sql",
)

INSERT_ORDERED_SQL_FILE = os.path.join(
    BASE_SQL_FILE_PATH, "article_text_insert_ordered.sql"
)
WEEKLY_DEDUP_SQL_FILE = os.path.join(
    BASE_SQL_FILE_PATH, "article_text_weekly_dedup.sql"
)


@task()
def get_source_keys() -> List[str]:
    """
    :return: List of S3 keys for the S3_BUCKET and SOURCE_PREFIX
    """
    logger = prefect.context.get("logger")

    file_list = S3List().run(bucket=S3_BUCKET, prefix=SOURCE_PREFIX)
    if len(file_list) == 0:
        raise Exception(
            f"No files to process for s3://{S3_BUCKET}/{SOURCE_PREFIX}. Ensure the firehose delivery stream delivering S3 files is writing objects."
        )

    if len(file_list) > NUM_FILES_PER_RUN:
        logger.warn(
            f"Number of files is greater than the number a worker can process in a single run. Found {len(file_list)} files, processing {NUM_FILES_PER_RUN}."
        )
        return file_list[0:NUM_FILES_PER_RUN]
    else:
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
    contents = S3Download().run(bucket=S3_BUCKET, key=key, compression="gzip")
    dicts = [json.loads(l) for l in contents.splitlines()]
    return pd.DataFrame.from_records(dicts)


@task()
def transform(df: pd.DataFrame) -> pd.DataFrame:
    df.rename(columns={"article": "html"}, inplace=True)
    df["text"] = [get_text_from_html(html) for html in df["html"]]
    df["text_md5"] = [hashlib.md5(t.encode("utf-8")).hexdigest() for t in df["text"]]
    return df


def get_stage_prefix() -> str:
    """
    The stage prefix is the destination for the transformed stage files used for Snowflake load
    :return: S3 Stage prefix for each run instance.
    """
    s3_prefix = STAGE_PREFIX
    flow_run_id = prefect.context.get("flow_run_id")
    return f"{s3_prefix}{flow_run_id}"


def stage_chunk(index: int, df: pd.DataFrame) -> str:
    bucket = S3_BUCKET
    s3_prefix = get_stage_prefix()
    key = f"{s3_prefix}/{index}.csv.gz"

    csv_buffer = BytesIO()
    with gzip.GzipFile(mode="w", fileobj=csv_buffer, compresslevel=1) as gz_file:
        df.to_csv(gz_file, index=False)

    s3 = boto3.resource("s3")
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
        raise Exception(f"No dataframes to stage.")

    df = pd.concat(dfs, sort=False)
    chunks = [(i, df[i : i + CHUNK_ROWS]) for i in range(0, df.shape[0], CHUNK_ROWS)]
    logger.info(f"Number of chunks created: {len(chunks)}.")
    keys = [stage_chunk(i, chunk) for i, chunk in chunks]
    logger.info(f"Staged keys: {*keys,}")
    return keys


@task()
def load(key: str):
    uri = f"s3://{S3_BUCKET}/{key}"
    logger = prefect.context.get("logger")
    logger.info(f"Snowflake loading key: {uri}")
    return SnowflakeQuery(**SNOWFLAKE_DEFAULT_DICT).run(
        data={"uri": uri}, query=IMPORT_SQL
    )


@task()
def cleanup(key: str):
    bucket = S3_BUCKET
    logger = prefect.context.get("logger")
    logger.info(f"deleting file: {str(key)}")
    s3 = boto3.resource("s3")
    s3.Object(bucket, key).delete()


@task()
def ordered_dataset_insert_transform() -> None:
    """This task will incrementally insert new records into the article_content_ordered_live table.
    This table was created manually from existing raw data as a starting point.
    Query is built so that this task is itempotent.
    """
    logger = prefect.context.get("logger")
    logger.info("Adding new records to article_content_ordered_live dataset...")
    result = SnowflakeQueriesFromFile(**FLOW_SNOWFLAKE_DICT).run(
        file_path=INSERT_ORDERED_SQL_FILE
    )
    logger.info(f"Inserted {result[1][0][0]} rows...")


@task()
def weekly_reordering_transform() -> None:
    """This task will perform a reordering of the article_content_ordered_live table weekly.
    This involves a new table being swaped with the old.
    Grants are cleaned up.
    Logic makes it that this will only run on Saturday if the last_ordered date is older than a week.
    A single run of this task updates the last _ordered date and prevents multiple runs.
    """
    logger = prefect.context.get("logger")
    logger.info("Checking to see if reordering is needed...")
    full_name = f"article_content_ordered_live"
    result = SnowflakeQuery(**FLOW_SNOWFLAKE_DICT).run(
        data={"full_name": full_name}, query=WEEKLY_DEDUP_CHECK_SQL
    )
    ready_for_reordering = result[0]
    current_dt = pendulum.now("UTC")
    # this should be Saturday
    if current_dt.day_of_week == 6 and ready_for_reordering:
        logger.info("Executing reordering of article_content_ordered_live dataset...")
        SnowflakeQueriesFromFile(**FLOW_SNOWFLAKE_DICT).run(
            file_path=WEEKLY_DEDUP_SQL_FILE
        )
    else:
        logger.info("Reordering not needed at this time...")


with Flow(
    FLOW_NAME,
    executor=LocalDaskExecutor(),
    schedule=get_interval_schedule(minutes=60),
) as flow:
    source_keys_results = get_source_keys()
    extract_results = extract.map(source_keys_results)
    transform_results = transform.map(extract_results)
    stage_results = stage(transform_results)
    load_results = load.map(stage_results)
    cleanup_task = cleanup.map(
        flatten([source_keys_results, stage_results])
    ).set_upstream(load_results)
    ordered_dataset_insert = ordered_dataset_insert_transform().set_upstream(
        cleanup_task
    )
    weekly_reordering_transform().set_upstream(ordered_dataset_insert)


if __name__ == "__main__":
    flow.schedule = None
    flow.run()
