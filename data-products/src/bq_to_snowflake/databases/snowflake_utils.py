from common.settings import NestedSettings, Settings
from prefect_snowflake import SnowflakeConnector, SnowflakeCredentials
from pydantic import SecretStr


class SnowflakeCredSettings(NestedSettings):
    account: str
    user: str
    private_key_passphrase: SecretStr
    private_key_path: str


class SnowflakeDbSettings(NestedSettings):
    database: str
    warehouse: str
    schema_name: str # schema is reserved word in Pydantic


class SnowflakeSettings(Settings):
    snowflake_credentials: SnowflakeCredSettings
    snowflake_connector: SnowflakeDbSettings


class PktSnowflakeConnector(SnowflakeConnector):
    def __init__(self, **data):
        settings = SnowflakeSettings() # type: ignore
        super().__init__(
            credentials = SnowflakeCredentials(**settings.snowflake_credentials.dict()),
            warehouse = settings.snowflake_connector.warehouse,
            schema = settings.snowflake_connector.schema_name,
            database = settings.snowflake_connector.database
        )
