import importlib
import json
import os
from pathlib import PosixPath

import common.cloud.gcp_utils as gcpu
import pytest
from prefect.blocks.fields import SecretDict


@pytest.mark.parametrize(
    "creds_type", [gcpu.MozGcpCredentials, gcpu.MozGcpCredentialsV2]
)
def test_moz_gcp(creds_type):
    x = creds_type()
    compare = x.dict()
    assert compare == {
        "service_account_file": PosixPath("tests/test.json"),
        "service_account_info": None,
        "project": "test",
        "_service_account_email": "test@test.iam.gserviceaccount.com",
        "block_type_slug": "gcp-credentials",
    }
    assert isinstance(x, creds_type)


@pytest.mark.parametrize(
    "creds_type", [gcpu.MozGcpCredentials, gcpu.MozGcpCredentialsV2]
)
def test_moz_gcp_with_sa_info(creds_type):
    envar_map = {
        "<class 'common.cloud.gcp_utils.MozGcpCredentials'>": "DF_CONFIG_GCP_CREDENTIALS",  # noqa: E501
        "<class 'common.cloud.gcp_utils.MozGcpCredentialsV2'>": "DF_CONFIG_GCP_CREDENTIALS_V2",  # noqa: E501
    }

    os.environ[envar_map[str(creds_type)]] = json.dumps(
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
    x = creds_type()
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
    assert isinstance(x, creds_type)
