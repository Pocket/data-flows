from shared.utils import SqlJob, get_files_for_cleanup

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
