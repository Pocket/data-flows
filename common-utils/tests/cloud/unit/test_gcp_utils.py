from pathlib import PosixPath

from prefect_gcp.bigquery import BigQueryWarehouse

from common.cloud.gcp_utils import PktBigQueryWarehouse


def test_pkt_big_query_warehouse():
    x = PktBigQueryWarehouse()
    compare = x.dict()
    compare.pop(
        "_connection"
    )  # popping _connection because its object and hard to compare  # noqa: E501
    assert compare == {
        "gcp_credentials": {
            "service_account_file": PosixPath("tests/test.json"),
            "service_account_info": None,
            "project": "test",
            "_service_account_email": "test@test.iam.gserviceaccount.com",
            "block_type_slug": "gcp-credentials",
        },
        "fetch_size": 1,
        "_unique_cursors": {},
        "block_type_slug": "bigquery-warehouse",
    }
    assert isinstance(x, BigQueryWarehouse)
