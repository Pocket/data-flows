"""
Flow to ingest Pocket parser article text into Snowflake.

This flow will:

- Grab s3 object payloads from "s3://pocket-data-items/article/streaming-html/".
- Produce a list of keys to download, with a max of 1000 per flow run.
- Download the objects into BytesIO objects.
- Transform each object into a Dataframe of resolved_ids.
- Combine the Dataframes into a single Dataframe of resolved_ids.
- Create chunks rows with a max of 5000 as a list of chunks.
- Upload each chuck as a single object to s3 as key of 
  f"{CS.deployment_type}/article/streaming-html-stage/{flow_run.id}/{list_index}.csv.gz".
- Ingest the s3 data into the raw.item.ARTICLE_CONTENT_V2 table.
- Insert ordered rows into the raw.item.ARTICLE_CONTENT_ORDERED_LIVE table.
- Clean up the source and staging objects used in the flow run.
- Perform a weekly reorder of the raw.item.ARTICLE_CONTENT_ORDERED_LIVE table.

DISCLAIMER:  This version was written as a direct migration to Prefect v2.
             Not much was changed, other than making it v2 compatible.

"""
import gzip
import hashlib
import json
import os
from io import BytesIO
from typing import Any

import pandas as pd
import pendulum as pdm
from common.databases.snowflake_utils import PktSnowflakeConnector, SnowflakeSettings
from common.deployment import FlowDeployment, FlowEnvar, FlowSpec
from common.settings import CommonSettings
from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run
from prefect.server.schemas.schedules import CronSchedule
from prefect_aws import AwsCredentials
from prefect_aws.s3 import s3_download, s3_list_objects, s3_upload
from prefect_dask.task_runners import DaskTaskRunner
from prefect_snowflake.database import (
    snowflake_multiquery,
    snowflake_query,
    snowflake_query_sync,
)
from shared.transform_utils import get_text_from_html

SFS = SnowflakeSettings()
CS = CommonSettings()  # type: ignore

DB = os.getenv("ARTICLE_DB", SFS.database)
SCHEMA = os.getenv("ARTICLE_SCHEMA", SFS.snowflake_schema)

# This bucket was created by another process. We may have to revisit using this bucket.
S3_BUCKET = os.getenv("ARTICLE_S3_BUCKET", "pocket-data-items-dev")
SOURCE_PREFIX = "article/streaming-html/"
STAGE_PREFIX = f"{CS.deployment_type}/article/streaming-html-stage/"
# Maximum number rows to include in a staging file.
# This is optimized for prefect import performance.
CHUNK_ROWS = 50000  # 3486 rows = 10MB
NUM_FILES_PER_RUN = int(os.getenv("ARTICLES_MAX_PER_RUN", 1000))

# Import from S3 to Snowflake
# 3.5k rows = 2 seconds on xsmall warehouse

# table should already exist, but this helps for testing in nonprod
# everything else is copied from the v1 flow sql files, except added the DB and SCHEMA
# variables to help with testing in nonprod
CREATE_TABLE_SQL = f"""
create TABLE if not exists {DB}.{SCHEMA}.ARTICLE_CONTENT_V2 (
	RESOLVED_ID NUMBER(38,0),
	HTML VARCHAR(16777216),
	TEXT VARCHAR(16777216),
	SNOWFLAKE_LOADED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	TEXT_MD5 VARCHAR(16777216)
);
"""

IMPORT_SQL = f"""
copy into {DB}.{SCHEMA}.ARTICLE_CONTENT_V2
(resolved_id, html, text, text_md5)
from %(uri)s
storage_integration = aws_integration_readonly_prod
file_format = (type = 'CSV', skip_header=1, FIELD_OPTIONALLY_ENCLOSED_BY='"')
on_error=ABORT_STATEMENT;
"""

CREATE_ORDERED_TABLE_SQL = f"""
create TABLE if not exists {DB}.{SCHEMA}.ARTICLE_CONTENT_ORDERED_LIVE (
	RESOLVED_ID NUMBER(38,0),
	HTML VARCHAR(16777216),
	TEXT VARCHAR(16777216),
	SNOWFLAKE_LOADED_AT TIMESTAMP_NTZ(9),
	TEXT_MD5 VARCHAR(16777216),
	LAST_ORDERED TIMESTAMP_LTZ(9)
);
"""

MAX_DATE = f"""
set max_date = (
    select max(snowflake_loaded_at) 
    from {DB}.{SCHEMA}.article_content_ordered_live
    );
"""

INSERT_ORDERED_SQL = f"""
insert into {DB}.{SCHEMA}.article_content_ordered_live (
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
from {DB}.{SCHEMA}.ARTICLE_CONTENT_V2
    where snowflake_loaded_at > $max_date
    order by resolved_id;
"""

REORDERING_SQL = f"""use warehouse dpt_wh_3xl;

create or replace table {DB}.{SCHEMA}.article_content_ordered_new as (
select RESOLVED_ID, 
HTML, 
TEXT, 
SNOWFLAKE_LOADED_AT,
TEXT_MD5,
current_timestamp() as last_ordered
from {DB}.{SCHEMA}.article_content_ordered_live
qualify row_number() over (partition by resolved_id order by snowflake_loaded_at desc) = 1
order by resolved_id);

-- apply proper grants to new ordered table before swap

grant select, delete on table {DB}.{SCHEMA}.article_content_ordered_new 
to role USER_DATA_DELETION_ROLE;
grant select on table {DB}.{SCHEMA}.article_content_ordered_new to role TRANSFORMER;
grant select on table {DB}.{SCHEMA}.article_content_ordered_new to role SELECT_ALL_ROLE;

-- swap and drop old table

alter table {DB}.{SCHEMA}.article_content_ordered_live swap with {DB}.{SCHEMA}.article_content_ordered_new;
drop table {DB}.{SCHEMA}.article_content_ordered_new;"""  # noqa: E501


# all s3 operations in tasks pulled out in favor of out of the box tasks
@task()
def get_keys_to_process(file_list: list[dict[str, Any]]) -> list[str]:
    """Return list of s3 keys to process.

    Args:
        file_list (dict[str, Any]): s3 keys available.

    Raises:
        Exception: This means no files to process and potential issue upstream(firehose).

    Returns:
        List[str]: s3 keys to process.
    """
    logger = get_run_logger()
    available_list = [x["Key"] for x in file_list]
    if len(file_list) == 0:
        raise Exception(
            f"No files to process for s3://{S3_BUCKET}/{SOURCE_PREFIX}. Ensure the firehose delivery stream delivering S3 files is writing objects."  # noqa: E501
        )

    if len(file_list) > NUM_FILES_PER_RUN:
        logger.warn(
            f"Number of files is greater than the number a worker can process in a single run. Found {len(file_list)} files, processing {NUM_FILES_PER_RUN}."  # noqa: E501
        )
        return available_list[0:NUM_FILES_PER_RUN]
    else:
        return available_list


# moved the gzip and some extract logic into the transform task
@task()
def transform(contents: bytes) -> pd.DataFrame:
    """
    - Transforms the html content for each row to text
    - Adds the transformed text data as a new field to the Dataframe

    Args:
        contents_future (str): s3 object to process

    Returns:
        pd.DataFrame: Transformed Dataframe
    """
    fileobj = BytesIO(contents)
    with gzip.GzipFile(fileobj=fileobj) as gzipfile:
        contents_str = gzipfile.read()
    dicts = [json.loads(c) for c in contents_str.splitlines()]
    df = pd.DataFrame.from_records(dicts)
    df.rename(columns={"article": "html"}, inplace=True)
    df["text"] = [get_text_from_html(html) for html in df["html"]]
    df["text_md5"] = [hashlib.md5(t.encode("utf-8")).hexdigest() for t in df["text"]]
    return df


# combined the stage and stage chunk tasks into new one
# also removed the get_stage_prefix function in favor of setting in the flow
@task()
def create_chunks(dfs: list[pd.DataFrame]) -> list[tuple]:
    """
    Stage files in S3 with a file size optimized for import.
    :return: S3 Stage file key.
    """
    logger = get_run_logger()
    logger.info(f"Staging dataframes count: {len(dfs)}")

    if len(dfs) == 0:
        raise Exception("No dataframes to stage.")

    df = pd.concat(dfs, sort=False)

    def prep_object_data(df: pd.DataFrame) -> BytesIO:
        csv_buffer = BytesIO()
        with gzip.GzipFile(mode="w", fileobj=csv_buffer, compresslevel=1) as gz_file:
            df.to_csv(gz_file, index=False)  # type: ignore

        return csv_buffer

    chunks = [
        (i, prep_object_data(df[i : i + CHUNK_ROWS]))
        for i in range(0, df.shape[0], CHUNK_ROWS)
    ]
    logger.info(f"Number of chunks created: {len(chunks)}.")
    return chunks


# using prefect_aws primitive for v2
@task()
def cleanup(key: str, aws_creds: AwsCredentials):
    logger = get_run_logger()
    logger.info(f"deleting file: {key}")
    s3_client = aws_creds.get_s3_client()
    s3_client.delete_object(Bucket=S3_BUCKET, Key=key)


# running with dask runner with explicit settings for mutliprocessing
@flow(task_runner=DaskTaskRunner())  # type: ignore
async def main():
    logger = get_run_logger()
    # sinlge object to pass to aws functions
    aws_creds = AwsCredentials()
    # using this for weekly reorder flag
    now = pdm.now("America/Los_Angeles")
    # get the objects available to process from s3
    source_keys = await s3_list_objects(
        bucket=S3_BUCKET, aws_credentials=aws_creds, prefix=SOURCE_PREFIX
    )
    # extract properly sized list of keys
    keys_to_process = get_keys_to_process(source_keys)
    # init list for collecting task results
    extract_jobs = []
    # submit s3 download tasks to dask
    for k in keys_to_process:
        extract_jobs.append(
            await s3_download.submit(
                key=k,  # type: ignore
                bucket=S3_BUCKET,  # type: ignore
                aws_credentials=aws_creds,  # type: ignore
            )
        )
    # collect downloaded fileobjs
    extract_results = [await e.result() for e in extract_jobs]
    # init list for collecting task results
    transform_jobs = []
    # submit transform for each fileobj
    for e in extract_results:
        transform_jobs.append(transform.submit(e))
    # collect dataframes produced by transform
    transform_results = [t.result() for t in transform_jobs]
    # create list of properly sized fileobjs
    chunks_created = create_chunks(transform_results)
    # init list for collecting task results
    upload_jobs = []
    # sumbit upload for each fileobj
    for i, c in chunks_created:
        upload_jobs.append(
            await s3_upload.submit(
                data=c.getvalue(),
                bucket=S3_BUCKET,
                key=os.path.join(STAGE_PREFIX, flow_run.id, f"{i}.csv.gz"),
                aws_credentials=aws_creds,
            )
        )
    # wailt for uploads to complete
    uploads_completed = [await u.result() for u in upload_jobs]
    # perform the snowflake statements needed for ingestion
    create_table = await snowflake_query(
        query=CREATE_TABLE_SQL,
        snowflake_connector=PktSnowflakeConnector(),
        wait_for=[uploads_completed],
    )  # type: ignore
    load = await snowflake_query(
        query=IMPORT_SQL,
        snowflake_connector=PktSnowflakeConnector(),
        params={"uri": f"s3://{S3_BUCKET}/{STAGE_PREFIX}"},
        wait_for=[create_table],
    )  # type: ignore
    # init list for collecting task results
    cleanup_jobs = []
    # since we are using the same bucket for nonprod
    # only clear source files if is_production
    if CS.is_production:  # pragma: no cover
        logger.info("Deleting source files...")
        for ktp in keys_to_process:
            cleanup_jobs.append(
                cleanup.submit(
                    key=ktp, aws_creds=aws_creds, wait_for=[load]
                )  # type: ignore
            )
    # clear the staged files
    for uc in uploads_completed:
        logger.info("Deleting loaded staging files...")
        cleanup_jobs.append(
            cleanup.submit(key=uc, aws_creds=aws_creds, wait_for=[load])  # type: ignore
        )
    # wait for cleanup jobs to complete
    cleanup_results = [cr.result() for cr in cleanup_jobs]
    # insert new records into the ordered table and metaflow uses
    ordered_dataset_insert = await snowflake_multiquery(
        queries=[CREATE_ORDERED_TABLE_SQL, MAX_DATE, INSERT_ORDERED_SQL],
        snowflake_connector=PktSnowflakeConnector(),
        wait_for=[cleanup_results],
    )  # type: ignore
    logger.info(
        f"Inserted {ordered_dataset_insert[2][0][0]} rows into ordererd table..."
    )
    # perform the weekly reorder of the ordered table
    # this if statement should ensure it only runs once since this
    # flow is scheduled hourly
    # TODO: will work with ML to see amount of data reordered can be scaled back
    if now.day_of_week == 0 and now.hour == 9:  # pragma: no cover
        logger.info("Running weekly reorder...")
        for q in list(filter(bool, REORDERING_SQL.split(";"))):
            await snowflake_query_sync(
                query=q, snowflake_connector=PktSnowflakeConnector()
            )


FLOW_SPEC = FlowSpec(
    flow=main,
    docker_env="base",
    secrets=[
        FlowEnvar(
            envar_name="DF_CONFIG_SNOWFLAKE_CREDENTIALS",
            envar_value=f"data-flows/{CS.deployment_type}/snowflake-credentials",
        ),
    ],
    deployments=[
        FlowDeployment(
            deployment_name="deployment",
            schedule=CronSchedule(cron="0 * * * *", timezone="America/Los_Angeles"),
            envars=[
                FlowEnvar(
                    envar_name="ARTICLE_DB",
                    envar_value=CS.deployment_type_value(
                        dev="development", staging="development", main="raw"
                    ),  # type: ignore
                ),
                FlowEnvar(
                    envar_name="ARTICLE_SCHEMA",
                    envar_value=CS.deployment_type_value(
                        dev="braun", staging="staging", main="item"
                    ),  # type: ignore
                ),
            ],
        ),
    ],
)

if __name__ == "__main__":
    from asyncio import run

    run(main())  # type: ignore
