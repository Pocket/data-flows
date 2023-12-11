from pathlib import Path
from typing import Optional

from common.settings import CommonSettings, NestedSettings, SecretsConfig, Settings
from prefect.blocks.fields import SecretDict
from prefect_gcp import BigQueryWarehouse, GcpCredentials

CS = CommonSettings()  # type: ignore


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

    gcp_credentials: Optional[GcpCredSettings]


class MozGcpSettings(Settings):
    """Settings for GCP Access.

    These must be set using DF_CONFIG_<field_name> envars.

    See pytest.ini at the common-utils root for examples.
    """

    gcp_credentials: Optional[GcpCredSettings]

    class Config(SecretsConfig):
        prefix = f"data-flows/{CS.deployment_type}"


class PktGcpCredentials(GcpCredentials):
    """Pocket GcpCrentials model with service account
    details already set.

    See https://prefecthq.github.io/prefect-gcp/ for usage.
    """

    def __init__(self, **data):
        settings = GcpSettings()  # type: ignore
        if x := settings.gcp_credentials:
            if xs := x.service_account_info:
                data["service_account_info"] = xs
            elif xs := x.service_account_file:
                data["service_account_file"] = xs
        super().__init__(**data)


class PktBigQueryWarehouse(BigQueryWarehouse):
    """Pocket BigQueryWarehouse model with credentials
    already set.

    See https://prefecthq.github.io/prefect-gcp/ for usage.
    """

    def __init__(self, **data):
        data["gcp_credentials"] = PktGcpCredentials()
        super().__init__(**data)


class MozGcpCredentials(GcpCredentials):
    """Mozilla GcpCrentials model with service account
    details already set if provided.  Application Default
    Credentials works just as well.

    See https://prefecthq.github.io/prefect-gcp/ for usage.
    """

    def __init__(self, **data):
        settings = SETTINGS
        if x := settings.gcp_credentials:
            if xs := x.service_account_info:
                data["service_account_info"] = xs
            elif xs := x.service_account_file:
                data["service_account_file"] = xs
        super().__init__(**data)


class MozBigQueryWarehouse(BigQueryWarehouse):
    """Pocket BigQueryWarehouse model with credentials
    already set.

    See https://prefecthq.github.io/prefect-gcp/ for usage.
    """

    def __init__(self, **data):
        data["gcp_credentials"] = MozGcpCredentials()
        super().__init__(**data)
