import importlib
import json
import os
from pathlib import PosixPath

import common.cloud.gcp_utils as gcpu
from prefect.blocks.fields import SecretDict


def test_moz_gcp():
    x = gcpu.MozGcpCredentials()
    compare = x.dict()
    assert compare == {
        "service_account_file": PosixPath("tests/test.json"),
        "service_account_info": None,
        "project": "test",
        "_service_account_email": "test@test.iam.gserviceaccount.com",
        "block_type_slug": "gcp-credentials",
    }
    assert isinstance(x, gcpu.MozGcpCredentials)


def test_moz_gcp_with_sa_info():
    os.environ["DF_CONFIG_GCP_CREDENTIALS"] = json.dumps(
        {
            "service_account_info": {
                "type": "service_account",
                "project_id": "test",
                "private_key_id": "key_foo",
                "private_key": "-----BEGIN EC PRIVATE KEY-----\nMHcCAQEEIIGb2np7v54Hs6++NiLE7CQtQg7rzm4znstHvrOUlcMMoAoGCCqGSM49\nAwEHoUQDQgAECvv0VyZS9nYOa8tdwKCbkNxlWgrAZVClhJXqrvOZHlH4N3d8Rplk\n2DEJvzp04eMxlHw1jm6JCs3iJR6KAokG+w==\n-----END EC PRIVATE KEY-----\n",  # noqa: E501
                "client_email": "test2@test.iam.gserviceaccount.com",
                "client_id": "123456789",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test%40test.iam.gserviceaccount.com",
                "universe_domain": "googleapis.com",
            }
        }
    )
    importlib.reload(gcpu)
    x = gcpu.MozGcpCredentials()
    compare = x.dict()
    compare["service_account_info"] = compare["service_account_info"].dict()
    assert compare == {
        "service_account_file": None,
        "service_account_info": SecretDict(
            {
                "type": "**********",
                "project_id": "**********",
                "private_key_id": "**********",
                "private_key": "**********",
                "client_email": "**********",
                "client_id": "**********",
                "auth_uri": "**********",
                "token_uri": "**********",
                "auth_provider_x509_cert_url": "**********",
                "client_x509_cert_url": "**********",
                "universe_domain": "**********",
            }
        ).dict(),
        "project": "test",
        "_service_account_email": "test2@test.iam.gserviceaccount.com",
        "block_type_slug": "gcp-credentials",
    }
    assert isinstance(x, gcpu.MozGcpCredentials)
