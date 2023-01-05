import gzip
import hashlib
import json
from io import BytesIO
from typing import List

import boto3
import pandas as pd
import pendulum
import prefect
import snowflake.connector as sf
from prefect import Flow, flatten, task
from prefect.executors import LocalDaskExecutor
from prefect.tasks.aws import S3Download, S3List
from prefect.tasks.snowflake import SnowflakeQuery

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

DATABASE_NAME = ARTICLES_DB_SCHEMA_DICT[ENVIRONMENT]["articles_database_name"]
SCHEMA_NAME = ARTICLES_DB_SCHEMA_DICT[ENVIRONMENT]["articles_schema_name"]

# SQL statements to insert new ordered raw data
# These depend on the initial execution of the 'initial clean' statements in the sql folder
# This will execute after the copy statement as a single statement

INSERT_ORDERED_SQL = f"""set max_date = (select max(snowflake_loaded_at) from {DATABASE_NAME}.{SCHEMA_NAME}.article_content_ordered_live);
        
    insert into {DATABASE_NAME}.{SCHEMA_NAME}.article_content_ordered_live (
        RESOLVED_ID, 
                HTML, 
                TEXT, 
                SNOWFLAKE_LOADED_AT,
                TEXT_MD5
    )
    select RESOLVED_ID, 
                HTML, 
                TEXT, 
                SNOWFLAKE_LOADED_AT,
                TEXT_MD5
    from raw.ITEM.ARTICLE_CONTENT_V2
        where snowflake_loaded_at > $max_date
        order by resolved_id;"""

# This will run after an hourly update only once on the weekend
# Will probably need to apply grants to the renamed live table
# Revoke grants will remove access to the old live table for better UX
WEEKLY_DEDUP_SQL = f"""
    -- weekly dedup on 3XL warehouse

    use warehouse dpt_wh_3xl;
    
    create or replace table {DATABASE_NAME}.{SCHEMA_NAME}.article_content_ordered_new as (
    select RESOLVED_ID, 
    HTML, 
    TEXT, 
    SNOWFLAKE_LOADED_AT,
    TEXT_MD5,
    current_timestamp() as last_ordered
    from {DATABASE_NAME}.{SCHEMA_NAME}.article_content_ordered_live
    qualify row_number() over (partition by resolved_id order by snowflake_loaded_at desc) = 1
    order by resolved_id);

    -- clean up ownership
    grant ownership on table {DATABASE_NAME}.{SCHEMA_NAME}.article_content_ordered_new to role LOADER REVOKE CURRENT GRANTS;

    -- apply proper grants to new ordered table before swap

    grant select, delete on table {DATABASE_NAME}.{SCHEMA_NAME}.article_content_ordered_new to role USER_DATA_DELETION_ROLE;
    grant all on table {DATABASE_NAME}.{SCHEMA_NAME}.article_content_ordered_new to role ML_SERVICE_ROLE;
    grant select on table {DATABASE_NAME}.{SCHEMA_NAME}.article_content_ordered_new to role TRANSFORMER;
    grant select on table {DATABASE_NAME}.{SCHEMA_NAME}.article_content_ordered_new to role SELECT_ALL_ROLE;

    -- swap and drop old table
    
    alter table {DATABASE_NAME}.{SCHEMA_NAME}.article_content_ordered_live swap with {DATABASE_NAME}.{SCHEMA_NAME}.article_content_ordered_new;
    drop table {DATABASE_NAME}.{SCHEMA_NAME}.article_content_ordered_new;
    
"""


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
    with sf.connect(**SNOWFLAKE_DEFAULT_DICT) as conn:
        result = conn.execute_string(INSERT_ORDERED_SQL)
        logger.info(f"Inserted {result[1].rowcount} rows...")


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
    with sf.connect(**SNOWFLAKE_DEFAULT_DICT) as conn:
        result = (
            conn.cursor()
            .execute(
                f"""select datediff('hours', max(last_ordered), current_timestamp()) > 168 as ready_for_reordering
   from {DATABASE_NAME}.{SCHEMA_NAME}.article_content_ordered_live;"""
            )
            .fetchone()
        )
        ready_for_reordering = result[0]
    current_dt = pendulum.now("UTC")
    # this should be Saturday
    if current_dt.day_of_week == 6 and ready_for_reordering:
        logger.info("Executing reordering of article_content_ordered_live dataset...")
        with sf.connect(**SNOWFLAKE_DEFAULT_DICT) as conn:
            conn.execute_string(WEEKLY_DEDUP_SQL)
    else:
        logger.info("Reordering not needed at this time...")


with Flow(
    FLOW_NAME,
    executor=LocalDaskExecutor(),
    schedule=get_interval_schedule(minutes=1440),
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
