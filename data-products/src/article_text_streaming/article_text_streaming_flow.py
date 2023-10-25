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
from pathlib import Path
from typing import Any

import pandas as pd
import pendulum as pdm
from common.databases.snowflake_utils import PktSnowflakeConnector
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

CS = CommonSettings()  # type: ignore

DB = os.getenv("ARTICLE_DB", "development")
SCHEMA = os.getenv("DF_CONFIG_SNOWFLAKE_SCHEMA")

# This bucket was created by another process. We may have to revisit using this bucket.
S3_BUCKET = os.getenv("ARTICLE_S3_BUCKET", "pocket-snowflake-dev")
SOURCE_PREFIX = "article/streaming-html/"
STAGE_PREFIX = f"{CS.deployment_type}/article/streaming-html-stage/"
# Maximum number rows to include in a staging file.
# This is optimized for prefect import performance.
CHUNK_ROWS = 50000  # 3486 rows = 10MB
NUM_FILES_PER_RUN = int(os.getenv("ARTICLES_MAX_PER_RUN", 50))
STORAGE_INTEGRATION = os.getenv(
    "ARTICLE_STORAGE_INTEGRATION", "AWSDEV_ACCOUNT_INTEGRATION"
)

# path for storing html2text errors
FAILURES_FILE_PATH = Path("/tmp/html2text_failures.json")

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

CREATE_FAILURES_TABLE_SQL = f"""
create TABLE if not exists {DB}.{SCHEMA}.ARTICLE_HTML2TEXT_FAILURES (
	FLOW_RUN_ID STRING,
    FLOW_RUN_NAME STRING,
    BATCH_ID NUMBER,
    RESOLVED_ID NUMBER,
    S3_KEY STRING,
    HTML VARIANT,
	SNOWFLAKE_LOADED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
);
"""

PUT_FAILURES = f"""PUT file://{FAILURES_FILE_PATH} @{DB}.{SCHEMA}.%ARTICLE_HTML2TEXT_FAILURES
OVERWRITE = TRUE"""  # noqa: E501

INSERT_FAILURES = f"""copy into {DB}.{SCHEMA}.ARTICLE_HTML2TEXT_FAILURES
(FLOW_RUN_ID, FLOW_RUN_NAME, BATCH_ID, RESOLVED_ID, S3_KEY, HTML)
FROM (
select %(flow_run_id)s, 
%(flow_run_name)s, 
$1:batch_id, 
$1:resolved_id,
$1:s3_key, 
$1:html
from @{DB}.{SCHEMA}.%%ARTICLE_HTML2TEXT_FAILURES)
file_format = (type = 'JSON', STRIP_OUTER_ARRAY = TRUE) 
;
"""

IMPORT_SQL = f"""
copy into {DB}.{SCHEMA}.ARTICLE_CONTENT_V2
(resolved_id, html, text, text_md5)
from %(uri)s
storage_integration = {STORAGE_INTEGRATION}
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
    select coalesce(max(snowflake_loaded_at), current_timestamp - interval '1 day')
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
    available_list = [x["Key"] for x in file_list]
    if len(file_list) == 0:
        raise Exception(
            f"No files to process for s3://{S3_BUCKET}/{SOURCE_PREFIX}. Ensure the firehose delivery stream delivering S3 files is writing objects."  # noqa: E501
        )
    return available_list


# moved the gzip and some extract logic into the transform task
@task()
def transform(
    contents: bytes, key: str, batch_id: int, failure_store: Path
) -> pd.DataFrame:
    """
    - Transforms the html content for each row to text
    - Adds the transformed text data as a new field to the Dataframe

    Args:
        contents_future (str): s3 object to process
        failure_store (list): shared list to track text conversion failures
        key (str): s3 key for contents being processed
        batch_id (int): batch_id for etl run

    Returns:
        pd.DataFrame: Transformed Dataframe
    """
    logger = get_run_logger()

    def handle_html2text(html: str, key: str, resolved_id: int):
        """Special helper function to skip failed conversions.

        Args:
            html (str): html to be converted to text
            resolved_id (list): resolved_id for content
            key (str): s3 key for contents being processed

        Returns:
            _type_: _description_
        """
        try:
            return get_text_from_html(html)
        except Exception:
            logger.error(f"{resolved_id} failed...")
            failure = {
                "resolved_id": resolved_id,
                "batch_id": batch_id,
                "s3_key": key,
                "html": html,
            }
            with open(failure_store, "a", newline="\n") as f:
                json.dump(failure, f)
                f.write("\n")

    fileobj = BytesIO(contents)
    with gzip.GzipFile(fileobj=fileobj) as gzipfile:
        contents_str = gzipfile.read()
    dicts = [json.loads(c) for c in contents_str.splitlines()]
    df = pd.DataFrame.from_records(dicts)
    df.rename(columns={"article": "html"}, inplace=True)
    df["text"] = [
        handle_html2text(html, key, resolved_id)
        for resolved_id, html in zip(df["resolved_id"], df["html"])
    ]
    df["text_md5"] = [
        hashlib.md5(t.encode("utf-8")).hexdigest() if t else None for t in df["text"]
    ]
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


@flow(task_runner=DaskTaskRunner())
async def etl(
    keys: list, batch_id: int, aws_creds: AwsCredentials, failure_store: Path
):
    logger = get_run_logger()
    # init list for collecting task results
    extract_jobs = []
    # submit s3 download tasks to dask
    for k in keys:
        extract_jobs.append(
            (
                await s3_download.submit(
                    key=k,  # type: ignore
                    bucket=S3_BUCKET,  # type: ignore
                    aws_credentials=aws_creds,  # type: ignore
                ),
                k,
            )
        )
    # collect downloaded fileobjs
    extract_results = [(await job.result(), key) for job, key in extract_jobs]
    # init list for collecting task results
    transform_jobs = []
    # submit transform for each fileobj
    for e in extract_results:
        contents, key = e
        transform_jobs.append(transform.submit(contents, key, batch_id, failure_store))
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
                key=os.path.join(
                    STAGE_PREFIX, flow_run.id, str(batch_id), f"{i}.csv.gz"
                ),
                aws_credentials=aws_creds,
            )  # type: ignore
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
    # only clear source files
    logger.info("Deleting source files...")
    for ktp in keys:
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


# running with dask runner with explicit settings for mutliprocessing
@flow()  # type: ignore
async def main():
    logger = get_run_logger()
    # sinlge object to pass to aws functions
    aws_creds = AwsCredentials()
    # using this for weekly reorder flag
    now = pdm.now("America/Los_Angeles")
    # create failure store
    failure_store = FAILURES_FILE_PATH
    failure_store.unlink(missing_ok=True)
    failure_store.touch()
    # get the objects available to process from s3
    source_keys = await s3_list_objects(
        bucket=S3_BUCKET, aws_credentials=aws_creds, prefix=SOURCE_PREFIX
    )

    # extract list of keys
    keys_to_process = get_keys_to_process(source_keys)

    # run etl in batches
    def batch(keys: list, n: int = NUM_FILES_PER_RUN):
        """Helper for batching keys for iterating.

        Args:
            keys (list): _description_
            n (_type_, optional): _description_. Defaults to NUM_FILES_PER_RUN.

        Yields:
            list: keys to process
        """
        for i in range(0, len(keys), n):
            yield keys[i : i + n]

    for idx, x in enumerate(batch(keys=keys_to_process)):
        logger.info(f"Processing batch of max {NUM_FILES_PER_RUN} files...")
        await etl(
            keys=x, batch_id=idx, aws_creds=aws_creds, failure_store=failure_store
        )
    # perform the bi-monthly reorder of the ordered table
    # this if statement should ensure it only runs once since this
    # flow is scheduled hourly
    if (
        now.week_of_month in [1, 3] and now.day_of_week == 0 and now.hour == 9
    ):  # pragma: no cover
        logger.info("Running weekly reorder...")
        for q in list(filter(bool, REORDERING_SQL.split(";"))):
            await snowflake_query_sync(
                query=q, snowflake_connector=PktSnowflakeConnector()
            )
    # create failures table if needed
    create_failures_table = await snowflake_query(
        query=CREATE_FAILURES_TABLE_SQL,
        snowflake_connector=PktSnowflakeConnector(),
    )
    # put file
    put_file = await snowflake_query_sync(
        query=PUT_FAILURES,
        snowflake_connector=PktSnowflakeConnector(),
        wait_for=[create_failures_table],
    )  # type: ignore
    # load failures to snowflake
    await snowflake_query(
        query=INSERT_FAILURES,
        snowflake_connector=PktSnowflakeConnector(),
        params={
            "flow_run_id": flow_run.id,
            "flow_run_name": flow_run.name,
        },
        wait_for=[put_file],
    )  # type: ignore
    # TODO: create a daily morning report showing number of failures


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
            cpu="4096",
            memory="8192",
            schedule=CronSchedule(cron="0 * * * *", timezone="America/Los_Angeles"),
            envars=[
                FlowEnvar(
                    envar_name="ARTICLE_DB",
                    envar_value=CS.deployment_type_value(
                        dev="development", staging="development", main="raw"
                    ),  # type: ignore
                ),
                FlowEnvar(
                    envar_name="DF_CONFIG_SNOWFLAKE_SCHEMA",
                    envar_value=CS.deployment_type_value(
                        dev="braun", staging="staging", main="item"
                    ),  # type: ignore
                ),
                FlowEnvar(
                    envar_name="ARTICLE_S3_BUCKET",
                    envar_value=CS.deployment_type_value(
                        dev="pocket-snowflake-dev",
                        staging="pocket-snowflake-dev",
                        main="pocket-data-items",
                    ),  # type: ignore
                ),
                FlowEnvar(
                    envar_name="ARTICLE_STORAGE_INTEGRATION",
                    envar_value=CS.deployment_type_value(
                        dev="AWSDEV_ACCOUNT_INTEGRATION",
                        staging="AWSDEV_ACCOUNT_INTEGRATION",
                        main="aws_integration_readonly_prod",
                    ),  # type: ignore
                ),
            ],
        ),
    ],
)

if __name__ == "__main__":
    from asyncio import run

    run(main())  # type: ignore
