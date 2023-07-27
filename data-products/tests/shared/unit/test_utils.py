import os
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

import pytest
from pendulum import now
from prefect import flow, task
from prefect_snowflake import SnowflakeCredentials
from pydantic import SecretStr
from shared.utils import QUERY_ENGINE_MAPPING, SqlJob, SqlStmt, get_files_for_cleanup

# create a fake list of files existing in a Snowflake stage
FAKE_FILE_LIST = [
    (
        "gcs://fake-stage/backend_events_for_mozilla/date=2023-06-17/time=22-00-00-000/data_0_0_0.snappy.parquet",
    ),
    (
        "gcs://fake-stage/backend_events_for_mozilla/date=2023-06-17/time=03-00-00-000/data_0_0_0.snappy.parquet",
    ),
    (
        "gcs://fake-stage/backend_events_for_mozilla/date=2023-06-18/time=00-00-00-000/data_0_0_0.snappy.parquet",
    ),
    (
        "gcs://fake-stage/backend_events_for_mozilla/date=2023-06-18/time=09-00-41-988/data_0_1_0.snappy.parquet",
    ),
    (
        "gcs://fake-stage/backend_events_for_mozilla/date=2023-06-18/time=16-00-41-988/data_0_2_0.snappy.parquet",
    ),
    (
        "gcs://fake-stage/backend_events_for_mozilla/date=2023-06-18/time=23-00-41-988/data_0_3_0.snappy.parquet",
    ),
    (
        "gcs://fake-stage/backend_events_for_mozilla/date=2023-06-19/time=22-00-41-988/data_0_0_0.snappy.parquet",
    ),
    (
        "gcs://fake-stage/backend_events_for_mozilla/date=2023-06-19/time=22-00-41-988/data_0_1_0.snappy.parquet",
    ),
    (
        "gcs://fake-stage/backend_events_for_mozilla/date=2023-06-19/time=22-00-41-988/data_0_2_0.snappy.parquet",
    ),
    (
        "gcs://fake-stage/backend_events_for_mozilla/date=2023-06-19/time=22-00-41-988/data_0_3_0.snappy.parquet",
    ),
    (
        "gcs://fake-stage/backend_events_for_mozilla/date=2023-06-20/time=22-00-41-988/data_0_0_0.snappy.parquet",
    ),
    (
        "gcs://fake-stage/backend_events_for_mozilla/date=2023-06-20/time=22-00-41-988/data_0_1_0.snappy.parquet",
    ),
    (
        "gcs://fake-stage/backend_events_for_mozilla/date=2023-06-20/time=23-00-41-988/data_0_2_0.snappy.parquet",
    ),
    (
        "gcs://fake-stage/backend_events_for_mozilla/date=2023-06-20/time=23-00-41-988/data_0_3_0.snappy.parquet",
    ),
    (
        "gcs://fake-stage/backend_events_for_mozilla/date=2023-06-21/time=00-00-00-000/data_0_3_0.snappy.parquet",
    ),
    (
        "gcs://fake-stage/backend_events_for_mozilla/date=2023-06-25/time=00-00-00-000/data_0_3_0.snappy.parquet",
    ),
    (
        "gcs://fake-stage/backend_events_for_mozilla/date=2023-06-30/time=00-00-00-000/data_0_3_0.snappy.parquet",
    ),
]

TEST_INTERVAL_SETS = [
    {
        "batch_start": "2023-06-17T21:59:59.999001Z",
        "batch_end": "2023-06-18T00:00:00Z",
        "first_interval_start": "2023-06-18T00:00:00Z",
        "sets_end": "2023-06-26T02:00:00Z",
        "is_initial": True,
        "is_final": False,
    },
    {
        "batch_start": "2023-06-18T00:00:00Z",
        "batch_end": "2023-06-19T00:00:00Z",
        "first_interval_start": "2023-06-18T00:00:00Z",
        "sets_end": "2023-06-26T02:00:00Z",
        "is_initial": False,
        "is_final": False,
    },
    {
        "batch_start": "2023-06-19T00:00:00Z",
        "batch_end": "2023-06-20T00:00:00Z",
        "first_interval_start": "2023-06-18T00:00:00Z",
        "sets_end": "2023-06-26T02:00:00Z",
        "is_initial": False,
        "is_final": False,
    },
    {
        "batch_start": "2023-06-20T00:00:00Z",
        "batch_end": "2023-06-21T00:00:00Z",
        "first_interval_start": "2023-06-18T00:00:00Z",
        "sets_end": "2023-06-26T02:00:00Z",
        "is_initial": False,
        "is_final": False,
    },
    {
        "batch_start": "2023-06-21T00:00:00Z",
        "batch_end": "2023-06-22T00:00:00Z",
        "first_interval_start": "2023-06-18T00:00:00Z",
        "sets_end": "2023-06-26T02:00:00Z",
        "is_initial": False,
        "is_final": False,
    },
    {
        "batch_start": "2023-06-22T00:00:00Z",
        "batch_end": "2023-06-23T00:00:00Z",
        "first_interval_start": "2023-06-18T00:00:00Z",
        "sets_end": "2023-06-26T02:00:00Z",
        "is_initial": False,
        "is_final": False,
    },
    {
        "batch_start": "2023-06-23T00:00:00Z",
        "batch_end": "2023-06-24T00:00:00Z",
        "first_interval_start": "2023-06-18T00:00:00Z",
        "sets_end": "2023-06-26T02:00:00Z",
        "is_initial": False,
        "is_final": False,
    },
    {
        "batch_start": "2023-06-24T00:00:00Z",
        "batch_end": "2023-06-25T00:00:00Z",
        "first_interval_start": "2023-06-18T00:00:00Z",
        "sets_end": "2023-06-26T02:00:00Z",
        "is_initial": False,
        "is_final": False,
    },
    {
        "batch_start": "2023-06-25T00:00:00Z",
        "batch_end": "2023-06-26T00:00:00Z",
        "first_interval_start": "2023-06-18T00:00:00Z",
        "sets_end": "2023-06-26T02:00:00Z",
        "is_initial": False,
        "is_final": False,
    },
    {
        "batch_start": "2023-06-26T00:00:00Z",
        "batch_end": "2023-06-26T02:00:00Z",
        "first_interval_start": "2023-06-18T00:00:00Z",
        "sets_end": "2023-06-26T02:00:00Z",
        "is_initial": False,
        "is_final": True,
    },
]


def test_get_files_for_cleanup():
    # create a generic SqlJob with override to replicate
    # backfill behavior.
    t = SqlJob(
        sql_folder_name="test",
        override_last_offset="2023-06-17 21:59:59.999",
        override_batch_end="2023-06-26",
    )  # type: ignore
    # SqlJob will now have intervals starting from override plus 1 ms.
    intervals = t.get_intervals()
    # we will add resulting lists to result list for assertion
    result_list = []
    for i in intervals:
        x = get_files_for_cleanup.fn(FAKE_FILE_LIST, i)
        # for each interval add the resulting cleanup list
        result_list.extend(x)
    # the final result should be deleting the 3 base date folders
    # and 2 files from 06/17/2023
    final = set(result_list) - set(
        [
            "date=2023-06-17/time=22-00-00-000",
            "date=2023-06-18",
            "date=2023-06-20",
            "date=2023-06-19",
            "date=2023-06-21",
            "date=2023-06-25",
        ]
    )
    assert final == set()


def test_intervals_1():
    """Expectations:
    if:
        - override_last_offset is set...
        - override_batch_end is set...
        - include_now=True...
        - override_batch_end is greater to last datetime in range

    then:
        - first interval batch start will be
          override_last_offset plus 1 microsecond
        - first interval batch end will be beginning of
          day following last offset day.
        - include_now should be ignored
          because of batch_end_override
          and batch_end_override value should be added to
          end of base intervals
    """
    t = SqlJob(
        sql_folder_name="test",
        override_last_offset="2023-06-17 21:59:59.999",
        override_batch_end="2023-06-26 02:00:00",
        include_now=True,
    ) # type: ignore
    assert [x.dict() for x in t.get_intervals()] == TEST_INTERVAL_SETS


def test_intervals_2():
    """Expectations:
    Same as _1 except batch override_batch_end is equal
    to last datetime in range.  Last set in list should be:

    {
            "batch_start": "2023-06-25T00:00:00Z",
            "batch_end": "2023-06-26T00:00:00Z",
            "first_interval_start": "2023-06-18T00:00:00Z",
            "sets_end": "2023-06-26T00:00:00Z",
            "is_initial": False,
            "is_final": True,
        }
    """
    t = SqlJob(
        sql_folder_name="test",
        override_last_offset="2023-06-17 21:59:59.999",
        override_batch_end="2023-06-26 00:00:00",
        include_now=True,
    ) # type: ignore
    test_list = deepcopy(TEST_INTERVAL_SETS)
    test_list.pop()
    test_list[-1] = {
        "batch_start": "2023-06-25T00:00:00Z",
        "batch_end": "2023-06-26T00:00:00Z",
        "first_interval_start": "2023-06-18T00:00:00Z",
        "sets_end": "2023-06-26T00:00:00Z",
        "is_initial": False,
        "is_final": True,
    }
    for i in test_list:
        i["sets_end"] = "2023-06-26T00:00:00Z"
    assert [x.dict() for x in t.get_intervals()] == test_list


def test_intervals_3():
    """Expectations:
    Same as _2 except include_now is False, should have no
    impact and return same list
    """
    t = SqlJob(
        sql_folder_name="test",
        override_last_offset="2023-06-17 21:59:59.999",
        override_batch_end="2023-06-26 00:00:00",
        include_now=False,
    ) # type: ignore
    test_list = deepcopy(TEST_INTERVAL_SETS)
    test_list.pop()
    test_list[-1] = {
        "batch_start": "2023-06-25T00:00:00Z",
        "batch_end": "2023-06-26T00:00:00Z",
        "first_interval_start": "2023-06-18T00:00:00Z",
        "sets_end": "2023-06-26T00:00:00Z",
        "is_initial": False,
        "is_final": True,
    }
    for i in test_list:
        i["sets_end"] = "2023-06-26T00:00:00Z"
    assert [x.dict() for x in t.get_intervals()] == test_list


LAST_OFFSET = now(tz="utc").subtract(days=3)
LAST_OFFSET_STR = LAST_OFFSET.to_iso8601_string()
FIRST_BATCH_START = LAST_OFFSET.add(microseconds=1)
FIRST_BATCH_END = LAST_OFFSET.end_of("day").add(microseconds=1)
FIRST_BATCH_START_STR = FIRST_BATCH_START.to_iso8601_string()
FIRST_BATCH_END_STR = FIRST_BATCH_END.to_iso8601_string()  # noqa: E501
SECOND_BATCH_END = FIRST_BATCH_END.add(days=1)
SECOND_BATCH_END_STR = SECOND_BATCH_END.to_iso8601_string()
SETS_END = FIRST_BATCH_END.add(days=2)
SETS_END_STR = SETS_END.to_iso8601_string()

DYNAMIC_TEST_INTERVAL_SETS = [
    {
        "batch_start": FIRST_BATCH_START_STR,
        "batch_end": FIRST_BATCH_END_STR,
        "first_interval_start": FIRST_BATCH_END_STR,
        "sets_end": SETS_END_STR,
        "is_initial": True,
        "is_final": False,
    },
    {
        "batch_start": FIRST_BATCH_END_STR,
        "batch_end": SECOND_BATCH_END_STR,
        "first_interval_start": FIRST_BATCH_END_STR,
        "sets_end": SETS_END_STR,
        "is_initial": False,
        "is_final": False,
    },
    {
        "batch_start": SECOND_BATCH_END_STR,
        "batch_end": SETS_END_STR,
        "first_interval_start": FIRST_BATCH_END_STR,
        "sets_end": SETS_END_STR,
        "is_initial": False,
        "is_final": True,
    },
]


DYNAMIC_TEST_EXTRA_INTERVAL = {
    "batch_start": SETS_END_STR,
    "batch_end": "no value yet",
    "first_interval_start": FIRST_BATCH_END_STR,
    "sets_end": "no value yet",
    "is_initial": False,
    "is_final": True,
}


def test_intervals_4():
    """Expectations:
    Same as _3 except include_now is False, except
    no override_batch_end.  Results should be the same
    minus the last list item in _3 will not be in _4
    """
    t = SqlJob(
        sql_folder_name="test",
        override_last_offset=LAST_OFFSET_STR,
        include_now=False,
    ) # type: ignore
    assert [x.dict() for x in t.get_intervals()] == DYNAMIC_TEST_INTERVAL_SETS


def test_intervals_5():
    """Expectations:
    Same as _4 except offset is passed to get_intervals.
    """
    t = SqlJob(
        sql_folder_name="test",
        include_now=False,
    ) # type: ignore
    assert [
        x.dict()
        for x in t.get_intervals(
            last_offset=LAST_OFFSET_STR,
        )
    ] == DYNAMIC_TEST_INTERVAL_SETS


def test_intervals_6():
    """Expectations:
    Same as _5 except include_now is True.  Results should be the same
    minus plus the new last list item through now utc.
    """
    t = SqlJob(
        sql_folder_name="test",
        override_last_offset=LAST_OFFSET_STR,
        include_now=True,
    ) # type: ignore
    interval_list = [x.dict() for x in t.get_intervals()]
    test_list = deepcopy(DYNAMIC_TEST_INTERVAL_SETS)
    extra_item = deepcopy(DYNAMIC_TEST_EXTRA_INTERVAL)
    interval_sets_end = interval_list[0]["sets_end"]
    extra_item["sets_end"] = interval_sets_end
    extra_item["batch_end"] = interval_sets_end
    test_list[-1]["is_final"] = False
    test_list.append(extra_item)
    for i in test_list:
        i["sets_end"] = interval_sets_end
    assert interval_list == test_list


def test_intervals_7():
    """Expectations:
    Same as _6 except offset is passed to get_intervals.
    """
    t = SqlJob(
        sql_folder_name="test",
        include_now=True,
    ) # type: ignore
    interval_list = [x.dict() for x in t.get_intervals(last_offset=LAST_OFFSET_STR)]
    test_list = deepcopy(DYNAMIC_TEST_INTERVAL_SETS)
    extra_item = deepcopy(DYNAMIC_TEST_EXTRA_INTERVAL)
    interval_sets_end = interval_list[0]["sets_end"]
    extra_item["sets_end"] = interval_sets_end
    extra_item["batch_end"] = interval_sets_end
    test_list[-1]["is_final"] = False
    test_list.append(extra_item)
    for i in test_list:
        i["sets_end"] = interval_sets_end
    assert interval_list == test_list


def test_intervals_no_offset():
    """Expectations:
    Same as _6 except offset is passed to get_intervals.
    """
    t = SqlJob(
        sql_folder_name="test",
        include_now=True,
    ) # type: ignore
    with pytest.raises(Exception) as e:
        t.get_intervals()
    assert (
        "The resulting last offset cannot be None. "
        "If last_offset is None, then initial_last_offset or "
        "override_last_offset must be set"
    ) in str(e.value)


@pytest.mark.asyncio
async def test_sql_stmt_gcp():
    sql = SqlStmt(
        sql_engine="bigquery",
        sql_text="select 1",
        is_multi_statement=False,
        connection_overrides={},
    )
    assert sql.standard_kwargs == {
        "gcp_credentials": QUERY_ENGINE_MAPPING["bigquery"]["gcp_credentials"](
            service_account_file="tests/test.json",
            service_account_info=None,
            project="test",
        ),
        "query": "select 1",
    }

    mock_state = {
        "call_count": 0,
    }

    @task
    async def fake_task(*args, **kwargs):
        mock_state["call_count"] += 1

    @flow
    async def fake_flow(*args, **kwargs):
        await sql.run_query_task("test")

    with patch("shared.utils.bigquery_query", fake_task):
        await fake_flow()
    assert mock_state["call_count"] == 1


@pytest.mark.asyncio
async def test_sql_stmt_sf():
    sql = SqlStmt(
        sql_engine="snowflake",
        sql_text="select 1",
        is_multi_statement=False,
        connection_overrides={},
    )
    assert sql.standard_kwargs == {
        "query": "select 1",
        "snowflake_connector": QUERY_ENGINE_MAPPING["snowflake"]["snowflake_connector"](
            credentials=SnowflakeCredentials(
                account="test.us-test-1",
                user="test@mozilla.com",
                password=None,
                private_key=None,
                private_key_path=Path("tmp/test.p8"),
                private_key_passphrase=SecretStr("**********"),
                authenticator="snowflake",
                token=None,
                endpoint=None,
                role="test",
                autocommit=None,
            ),
            database="development",
            warehouse="prefect_wh_test",
            fetch_size=1,
            poll_frequency_s=1,
        ),
    }

    mock_state = {
        "call_count": 0,
    }

    @task
    async def fake_task(*args, **kwargs):
        mock_state["call_count"] += 1

    @flow
    async def fake_flow(*args, **kwargs):
        await sql.run_query_task("test")

    with patch("shared.utils.snowflake_query", fake_task):
        await fake_flow()
    assert mock_state["call_count"] == 1


@pytest.mark.asyncio
async def test_sql_stmt_snowflake_mulit():
    sql = SqlStmt(
        sql_engine="snowflake",
        sql_text="select 1; select 2; select3;",
        is_multi_statement=True,
        connection_overrides={},
    )
    assert sql.standard_kwargs == {
        "query": "select 1; select 2; select3;",
        "snowflake_connector": QUERY_ENGINE_MAPPING["snowflake"]["snowflake_connector"](
            credentials=SnowflakeCredentials(
                account="test.us-test-1",
                user="test@mozilla.com",
                password=None,
                private_key=None,
                private_key_path=Path("tmp/test.p8"),
                private_key_passphrase=SecretStr("**********"),
                authenticator="snowflake",
                token=None,
                endpoint=None,
                role="test",
                autocommit=None,
            ),
            database="development",
            warehouse="prefect_wh_test",
            fetch_size=1,
            poll_frequency_s=1,
        ),
    }

    mock_state = {
        "call_count": 0,
    }

    @task
    async def fake_task(*args, **kwargs):
        mock_state["call_count"] += 1

    @flow
    async def fake_flow(*args, **kwargs):
        await sql.run_query_task("test")

    with patch("shared.utils.snowflake_multiquery", fake_task):
        await fake_flow()
    assert mock_state["call_count"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("sql_engine", ["mysql", "postgres"])
async def test_sql_stmt_sqlalchemy(sql_engine):
    os.environ[
        "DF_CONFIG_SQLALCHEMY_URL"
    ] = f"{sql_engine}://scott:tiger@localhost:5432"
    sql = SqlStmt(
        sql_engine="mysql",
        sql_text="select 1",
        is_multi_statement=False,
        connection_overrides={},
    )
    assert sql.standard_kwargs == {
        "sqlalchemy_credentials": QUERY_ENGINE_MAPPING[sql_engine][
            "sqlalchemy_credentials"
        ](
            url="test",
        ),
        "query": "select 1",
    }

    mock_state = {
        "call_count": 0,
    }

    @task
    async def fake_task(*args, **kwargs):
        mock_state["call_count"] += 1

    @flow
    async def fake_flow(*args, **kwargs):
        await sql.run_query_task("test")

    with patch("shared.utils.sqlalchemy_execute", fake_task):
        await fake_flow()
    assert mock_state["call_count"] == 1


def test_no_sql_engine():
    t = SqlJob(sql_folder_name="test_bad_engine") # type: ignore
    with pytest.raises(Exception) as e:
        t.render_sql_file("data.sql")
    assert (
        'SQL file must have jinja2 block {% set sql_engine = "<engine_type>" %}, '
        "and must be one of "
        "dict_keys(['snowflake', 'bigquery', 'postgres', 'mysql'])"
    ) in str(e.value)
