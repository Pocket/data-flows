import json
import os
from pathlib import PosixPath

from common.cloud.gcp_utils import PktBigQueryWarehouse
from prefect.blocks.fields import SecretDict
from prefect_gcp.bigquery import BigQueryWarehouse


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


def test_pkt_big_query_warehouse_with_sa_info():
    os.environ["DF_CONFIG_GCP_CREDENTIALS"] = json.dumps(
        {
            "service_account_info": {
                "type": "service_account",
                "project_id": "test",
                "private_key_id": "key_foo",
                "private_key": "-----BEGIN EC PRIVATE KEY-----\nMHcCAQEEIIGb2np7v54Hs6++NiLE7CQtQg7rzm4znstHvrOUlcMMoAoGCCqGSM49\nAwEHoUQDQgAECvv0VyZS9nYOa8tdwKCbkNxlWgrAZVClhJXqrvOZHlH4N3d8Rplk\n2DEJvzp04eMxlHw1jm6JCs3iJR6KAokG+w==\n-----END EC PRIVATE KEY-----\n",  # noqa: E501
                "client_email": "test@test.iam.gserviceaccount.com",
                "client_id": "123456789",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test%40test.iam.gserviceaccount.com",
                "universe_domain": "googleapis.com",
            }
        }
    )
    x = PktBigQueryWarehouse()
    compare = x.dict()
    assert compare["gcp_credentials"]["service_account_file"] is None
    assert isinstance(x, BigQueryWarehouse)
    assert isinstance(compare["gcp_credentials"]["service_account_info"], SecretDict)
