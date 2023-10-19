import gzip
import json
import os
from asyncio import run
from collections import Counter
from copy import deepcopy
from io import BytesIO
from unittest.mock import MagicMock, Mock

import boto3
import pytest
from article_text_streaming.article_text_streaming_flow import (
    FAILURES_FILE_PATH,
    S3_BUCKET,
    SOURCE_PREFIX,
    STAGE_PREFIX,
    cleanup,
    create_chunks,
    etl,
    main,
)
from common import get_script_path
from common.settings import CommonSettings
from moto import mock_s3
from prefect import flow, get_run_logger, task
from prefect_aws import AwsCredentials

CS = CommonSettings()  # type: ignore


def write_gzip_data(text: str) -> BytesIO:
    """Helper function to create gzipped fileobj

    Args:
        text (str): str to add to fileobj

    Returns:
        BytesIO: Retruns a fileobj
    """
    file_buffer = BytesIO()
    with gzip.GzipFile(mode="w", fileobj=file_buffer, compresslevel=1) as gz_file:
        gz_file.write(text.encode())  # type: ignore
    return file_buffer


def create_test_data(end_range: int = 100) -> dict[str, BytesIO]:
    """Helper function to create fileobjs.
    Produces a dictionary with s3 style key
    and value of gzipped fileobj.  This is used to upload
    via moto and to mock s3 download.

    Args:
        end_range (int): Optional to set the end range

    Returns:
        dict[str, BytesIO]: dict containing gzipped test data.
    """
    # generate the test data
    with open(os.path.join(get_script_path(), "test.json")) as f:
        test_dict = json.load(f)
    results = {}
    for i in range(1, end_range):
        new_id = int(test_dict["resolved_id"]) + i
        new_dict = deepcopy(test_dict)
        new_dict["resolved_id"] = new_id
        dict_key = os.path.join(SOURCE_PREFIX, f"{new_id}.gz")
        results[dict_key] = write_gzip_data(json.dumps(new_dict))
    return results


# setting max number of files to 25 via envar
@pytest.mark.parametrize(
    "range_end",
    [1, 2, 50],
)
def test_main(monkeypatch, range_end):
    # generate test data
    test_data = create_test_data(range_end)
    # run mocks for s3 upload
    with mock_s3():
        # prep the bucket
        s3m = boto3.client("s3", region_name="us-east-1")
        s3m.create_bucket(Bucket=S3_BUCKET)

        # load data into mock bucket
        for k, v in test_data.items():
            test_obj = BytesIO(v.getvalue())
            s3m.upload_fileobj(
                Bucket=S3_BUCKET,
                Key=k,
                Fileobj=test_obj,
            )

        # mock snowflake query tasks
        @task
        async def fake_task(*args, **kwargs):
            logger = get_run_logger()
            logger.info("Ran Fake Snowflake Query!")
            return [[], [], [(9999,)]]

        monkeypatch.setattr(
            "article_text_streaming.article_text_streaming_flow.snowflake_multiquery",
            fake_task,
        )
        monkeypatch.setattr(
            "article_text_streaming.article_text_streaming_flow.snowflake_query",
            fake_task,
        )
        monkeypatch.setattr(
            "article_text_streaming.article_text_streaming_flow.snowflake_query_sync",
            fake_task,
        )

        # mock s3 download
        @task
        async def fake_s3_download(key, *args, **kwargs):
            x = test_data[key]
            return x.getvalue()

        monkeypatch.setattr(
            "article_text_streaming.article_text_streaming_flow.s3_download",
            fake_s3_download,
        )

        # mock s3 upload
        @task
        async def fake_s3_upload(key, *args, **kwargs):
            logger = get_run_logger()
            logger.info(f"Uploading {key} to fake s3!")
            return key

        monkeypatch.setattr(
            "article_text_streaming.article_text_streaming_flow.s3_upload",
            fake_s3_upload,
        )

        # mock s3 delete and assert
        @task
        def fake_s3_delete(key, *args, **kwargs):
            logger = get_run_logger()
            logger.info(f"deleting file: {key}")
            path = os.path.join(STAGE_PREFIX, "1")
            assert path in key or key in test_data.keys()

        monkeypatch.setattr(
            "article_text_streaming.article_text_streaming_flow.cleanup",
            fake_s3_delete,
        )

        # patch flow_run id
        monkeypatch.setattr(
            "article_text_streaming.article_text_streaming_flow.flow_run",
            MagicMock(id="1"),
        )

        none_type_count = 1
        if CS.is_production:
            none_type_count = range_end - 1

        # run flow tests
        # leveraging the result states from the flow to assert

        # test subflow
        if range_end == 50:
            keys = list(test_data.keys())
            result_states = run(etl(keys, 0, AwsCredentials(), FAILURES_FILE_PATH))
            results_list = [
                rs.data.artifact_description for rs in result_states  # type: ignore
            ]
            results_counter = Counter(results_list)
            assert results_counter["Unpersisted result of type `list`"] == 4
            assert (
                results_counter["Unpersisted result of type `NoneType`"]
                == none_type_count
            )
            assert (
                results_counter["Unpersisted result of type `bytes`"] == range_end - 1
            )
            assert (
                results_counter["Unpersisted result of type `DataFrame`"]
                == range_end - 1
            )

        # test main flow
        if range_end == 1:
            with pytest.raises(Exception):
                run(main())  # type: ignore
        else:
            list_count = 6
            if range_end == 50:
                list_count += 1
            result_states = run(main())  # type: ignore
            results_list = [rs.data.artifact_description for rs in result_states]  # type: ignore
            results_counter = Counter(results_list)
            assert results_counter["Unpersisted result of type `list`"] == list_count

            # test failures
            monkeypatch.setattr(
                "article_text_streaming.article_text_streaming_flow.get_text_from_html",
                Mock(side_effect=Exception()),
            )
            run(main())
            test_text = FAILURES_FILE_PATH.read_text()
            assert len(test_text.splitlines()) == range_end - 1


def test_create_chunks_exception():
    with pytest.raises(Exception):

        @flow
        def test_flow():
            create_chunks([])

        test_flow()


def test_cleanup():
    with mock_s3():
        # prep the bucket
        s3m = boto3.client("s3", region_name="us-east-1")
        s3m.create_bucket(Bucket=S3_BUCKET)

        # load data into mock bucket
        test_obj = BytesIO("test".encode())
        s3m.upload_fileobj(
            Bucket=S3_BUCKET,
            Key="test.json",
            Fileobj=test_obj,
        )

        @flow
        def test_flow():
            cleanup.fn("test.json", AwsCredentials())

        test_flow()

        files = s3m.list_objects_v2(Bucket=S3_BUCKET)
        assert files["KeyCount"] == 0
