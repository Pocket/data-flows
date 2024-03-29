import os
from pathlib import Path
from typing import Optional

from prefect.blocks.fields import SecretDict
from prefect_gcp import GcpCredentials

from common.settings import (
    NestedSettings,
    SecretSettings,
    get_cached_settings,
)


class GcpCredSettings(NestedSettings):
    """Nested Settings model for GCP (Google Cloud Platform)
    credentials.  Meant to be used in GCPSettings model.
    """

    service_account_info: Optional[SecretDict]
    service_account_file: Optional[Path]


class GcpSettings(SecretSettings):
    """Settings for GCP Access.

    These must be set using DF_CONFIG_<field_name> envars.

    See pytest.ini at the common-utils root for examples.
    """

    gcp_credentials: Optional[GcpCredSettings]


class GcpSettingsV2(SecretSettings):
    """Settings for GCP Access.

    These must be set using DF_CONFIG_<field_name> envars.

    V2 to support access to mozsocial-dw project.

    See pytest.ini at the common-utils root for examples.
    """

    gcp_credentials_v2: Optional[GcpCredSettings]


class MozGcpCredentials(GcpCredentials):
    """Mozilla GcpCrentials model with service account
    details already set if provided.  Application Default
    Credentials works just as well.

    See https://prefecthq.github.io/prefect-gcp/ for usage.
    """

    def __init__(self, **data):
        if not os.getenv("DF_CONFIG_IGNORE_GCP_SETTINGS"):
            settings = get_cached_settings(GcpSettings)
            if x := settings.gcp_credentials:
                if xs := x.service_account_info:
                    data["service_account_info"] = xs
                elif xs := x.service_account_file:
                    data["service_account_file"] = xs
        super().__init__(**data)


class MozGcpCredentialsV2(GcpCredentials):
    """Mozilla GcpCrentials model with service account
    details already set if provided.  Application Default
    Credentials works just as well.

    V2 to support access to mozsocial-dw project.

    See https://prefecthq.github.io/prefect-gcp/ for usage.
    """

    def __init__(self, **data):
        if not os.getenv("DF_CONFIG_IGNORE_GCP_SETTINGS"):
            settings = get_cached_settings(GcpSettingsV2)
            if x := settings.gcp_credentials_v2:
                if xs := x.service_account_info:
                    data["service_account_info"] = xs
                elif xs := x.service_account_file:
                    data["service_account_file"] = xs
        super().__init__(**data)
