from pathlib import Path
from typing import Optional

from common.settings import NestedSettings, Settings
from prefect.blocks.fields import SecretDict
from prefect_gcp import BigQueryWarehouse, GcpCredentials


class GcpCredSettings(NestedSettings):
    """Nested Settings model for GCP (Google Cloud Platform)
    credentials.  Meant to be used in GCPSettings model.
    """

    service_account_info: Optional[SecretDict]
    service_account_file: Optional[Path]


class GcpSettings(Settings):
    """Settings for GCP Access.

    These must be set using DF_CONFIG_<field_name> envars.

    See pytest.ini at the common-utils root for examples.
    """

    gcp_credentials: GcpCredSettings


class PktGcpCredentials(GcpCredentials):
    """Pocket GcpCrentials model with service account
    details already set.

    See https://prefecthq.github.io/prefect-gcp/ for usage.
    """

    def __init__(self, **data):
        settings = GcpSettings()  # type: ignore
        data["service_account_info"] = settings.gcp_credentials.service_account_info
        data["service_account_file"] = settings.gcp_credentials.service_account_file
        super().__init__(**data)


class PktBigQueryWarehouse(BigQueryWarehouse):
    """Pocket BigQueryWarehouse model with credentials
    already set.

    See https://prefecthq.github.io/prefect-gcp/ for usage.
    """

    def __init__(self, **data):
        data["gcp_credentials"] = PktGcpCredentials()
        super().__init__(**data)
