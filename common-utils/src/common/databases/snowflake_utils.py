from typing import Optional

from common.settings import CommonSettings, NestedSettings, Settings
from prefect_snowflake import SnowflakeConnector, SnowflakeCredentials
from pydantic import PrivateAttr, SecretBytes, SecretStr, constr


class SnowflakeCredSettings(NestedSettings):
    """Settings that can be passed in for
    Snowflake Credentials.  This is mean to be used as
    a sub model of the SnowflakeSetting model.
    """

    account: str
    user: str
    private_key_passphrase: Optional[SecretStr]
    private_key: Optional[SecretBytes]
    private_key_path: Optional[str]
    role: str


# we want to make sure proper warehouses are used
# could be changed to a Literal list or enum later
WAREHOUSE_REGEX = "^(?i)prefect.*?"


class SnowflakeSettings(Settings):
    """Settings for Snowflake Connection.  Database can only be 'development'
    or 'prefect' depending on CommonSetings.dev_or_production property value.

    These must be set using DF_CONFIG_<field_name> envars.

    This is for the initial connection only.  Normal Snowflake
    methods for changing things on execution still work.

    See pytest.ini at the common-utils root for examples.
    """

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
    """Pocket version of the Snowflake Connector provided
    by Prefect-Snowflake with credentials and other settings already applied.
    Schema and Warehouse can be set explicitly here, like this:

    CS = CommonSettings()  # type: ignore
    SFC = PktSnowflakeConnector(
        schema="public", warehouse=f"prefect_wh_{CS.dev_or_production}"
    )

    Warehouse is overriden to enforce our regex.

    See https://prefecthq.github.io/prefect-snowflake/ for usage.
    """

    warehouse: constr(regex=WAREHOUSE_REGEX)  # type: ignore

    def __init__(self, **data):
        """Set credentials and other settings on usage of
        model.  SnowflakeSettings values for schema and warehouse
        are given preference.
        """
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
    """Return the proper Snowflake to GCS (Google Cloud Storage) for Prefect Flows.
    Based on deployment type.

    Returns:
        str: Full 3 part stage name.
    """
    cs = CommonSettings()  # type: ignore
    stage = "DEVELOPMENT.PUBLIC.PREFECT_GCS_STAGE_PARQ_DEV"
    if cs.is_production:
        stage = "PREFECT.PUBLIC.PREFECT_GCS_STAGE_PARQ_PROD"
    return stage
