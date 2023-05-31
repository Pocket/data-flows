from typing import Optional

from prefect_snowflake import SnowflakeConnector, SnowflakeCredentials
from pydantic import PrivateAttr, SecretBytes, SecretStr, constr

from common.settings import CommonSettings, NestedSettings, Settings


class SnowflakeCredSettings(NestedSettings):
    account: str
    user: str
    private_key_passphrase: Optional[SecretStr]
    private_key: Optional[SecretBytes]
    private_key_path: Optional[str]
    role: str


WAREHOUSE_REGEX = "^(?i)prefect.*?"


class SnowflakeSettings(Settings):
    snowflake_credentials: SnowflakeCredSettings
    _database: str = PrivateAttr()
    snowflake_warehouse: Optional[constr(regex=WAREHOUSE_REGEX)]  # type: ignore
    snowflake_schema: Optional[str]

    def __init__(self, **data):
        super().__init__(**data)
        db = "development"
        cs = CommonSettings()  # type: ignore
        if cs.is_production:
            db = "prefect"
        self._database = db

    @property
    def database(self):
        return self._database


class PktSnowflakeConnector(SnowflakeConnector):
    warehouse: constr(regex=WAREHOUSE_REGEX)  # type: ignore

    def __init__(self, **data):
        settings = SnowflakeSettings()  # type: ignore
        data["database"] = settings.database
        data["credentials"] = SnowflakeCredentials(
            **settings.snowflake_credentials.dict()
        )
        if x := settings.snowflake_schema:
            data["schema"] = x
        if x := settings.snowflake_warehouse:
            data["warehouse"] = x
        super().__init__(**data)


def get_gcs_stage():
    cs = CommonSettings()  # type: ignore
    stage = "DEVELOPMENT.PUBLIC.PREFECT_GCS_STAGE_PARQ_DEV"
    if cs.is_production:
        stage = "PREFECT.PUBLIC.PREFECT_GCS_STAGE_PARQ_PROD"
    return stage
