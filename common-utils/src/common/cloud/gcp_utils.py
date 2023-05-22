from common.settings import NestedSettings, Settings
from prefect.blocks.fields import SecretDict
from prefect_gcp import BigQueryWarehouse, GcpCredentials
from pathlib import Path
from typing import Optional


class GcpCredSettings(NestedSettings):
    service_account_info: Optional[SecretDict]
    service_account_file: Optional[Path]


class GcpSettings(Settings):
    gcp_credentials: GcpCredSettings


class PktGcpCredentials(GcpCredentials):
    def __init__(self, **data):
        settings = GcpSettings()  # type: ignore
        super().__init__(
            service_account_info=settings.gcp_credentials.service_account_info,
            service_account_file=settings.gcp_credentials.service_account_file,
        )


class PktBigQueryWarehouse(BigQueryWarehouse):
    def __init__(self, **data):
        super().__init__(gcp_credentials=PktGcpCredentials())
