import os
from typing import Optional, Union

from prefect import task
from prefect_snowflake import SnowflakeConnector, SnowflakeCredentials
from pydantic import BaseModel, PrivateAttr, SecretBytes, SecretStr, constr

from common.settings import CommonSettings, NestedSettings, Settings

CS = CommonSettings()  # type: ignore


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
    snowflake_warehouse: constr(regex=WAREHOUSE_REGEX) = f"prefect_wh_{CS.dev_or_production}"  # type: ignore  # noqa: E501
    snowflake_schema: str = "public"
    snowflake_gcp_stages: dict

    def __init__(self, **data):
        super().__init__(**data)
        db = "development"
        if CS.is_production:
            db = "prefect"
        self._database = db

    @property
    def database(self):
        return self._database


class PktSnowflakeConnector(SnowflakeConnector):
    """Pocket version of the Snowflake Connector provided
    by Prefect-Snowflake with credentials and other settings already applied.
    schema and warehouse must be set via envars.
    All other base model attributes can be set explicitly here.

    Warehouse is overriden to enforce our regex in settings.

    See https://prefecthq.github.io/prefect-snowflake/ for usage.
    """

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
        data["schema"] = settings.snowflake_schema
        data["warehouse"] = settings.snowflake_warehouse
        super().__init__(**data)


class SfGcsStage(BaseModel):
    stage_name: str
    stage_location: str

    def __str__(self):
        return self.stage_name


def get_gcs_stage(stage_id: str = "default") -> SfGcsStage:
    """Return the proper Snowflake to GCS (Google Cloud Storage) for Prefect Flows.
    Based on deployment type.  Will take in an optional stage id as
    a escape hatch for special circumstances.

    Returns:
        str: Full 3 part stage name.
    """

    stage_config = SnowflakeSettings().snowflake_gcp_stages  # type: ignore
    return SfGcsStage(
        stage_name=stage_config[f"{stage_id}_name"],
        stage_location=stage_config[f"{stage_id}_location"],
    )


@task()
def get_pocket_snowflake_connector_block(
    warehouse_override: Union[str, None] = None,
    schema_override: Union[str, None] = None,
    **kwargs,
):
    if x := warehouse_override:
        os.environ["DF_CONFIG_SNOWFLAKE_WAREHOUSE"] = x
    if x := schema_override:
        os.environ["DF_CONFIG_SNOWFLAKE_SCHEMA"] = x
    return PktSnowflakeConnector(**kwargs)
