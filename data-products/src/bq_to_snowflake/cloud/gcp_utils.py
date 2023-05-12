from common.settings import NestedSettings, Settings
from prefect.blocks.fields import SecretDict
from prefect_gcp import BigQueryWarehouse, GcpCredentials


class GcpCredSettings(NestedSettings):
    service_account_info: SecretDict
    project: str


class GcpSettings(Settings):
    gcp_credentials: GcpCredSettings


SETTINGS = GcpSettings()  # type: ignore


class PktGcpCredentials(GcpCredentials):
    def __init__(self):
        super().__init__(
            service_account_info=SETTINGS.gcp_credentials.service_account_info,
            project=SETTINGS.gcp_credentials.project,
        )


class PktBigQueryWarehouse(BigQueryWarehouse):
    def __init__(self):
        super().__init__(gcp_credentials=PktGcpCredentials())
