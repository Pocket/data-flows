import asyncio
from typing import Any, Optional, Union

from common.settings import CS, NestedSettings, SecretSettings, get_cached_settings
from prefect import task
from prefect_snowflake import SnowflakeConnector, SnowflakeCredentials
from pydantic import BaseModel, PrivateAttr, SecretBytes, SecretStr, constr
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor


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


class SnowflakeGcsStageData(NestedSettings):
    default_name: str
    default_location: str
    gcs_pocket_shared_name: str
    gcs_pocket_shared_location: str


class SnowflakeGcsStageSettings(SecretSettings):
    snowflake_gcp_stage_data: SnowflakeGcsStageData


class SnowflakeSettings(SecretSettings):
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

    def __init__(self, **data):
        super().__init__(**data)
        db = "development"
        if CS.is_production:
            db = "prefect"
        self._database = db

    @property
    def database(self):
        return self._database

    class Config:
        secret_fields = ["snowflake_credentials"]


class MozSnowflakeConnector(SnowflakeConnector):
    """Mozilla version of the Snowflake Connector provided
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
        settings = get_cached_settings(SnowflakeSettings)
        data["database"] = settings.database
        data["credentials"] = SnowflakeCredentials(
            **settings.snowflake_credentials.dict()
        )

        schema = data.get("schema", settings.snowflake_schema)
        warehouse = data.get("warehouse", settings.snowflake_warehouse)

        data["schema"] = schema
        data["warehouse"] = warehouse
        super().__init__(**data)


class SfGcsStage(BaseModel):
    stage_name: str
    stage_location: str

    def __str__(self):
        return self.stage_name


def get_gcs_stage(
    stage_config: SnowflakeGcsStageData, stage_id: str = "default"
) -> SfGcsStage:
    """Return the proper Snowflake to GCS (Google Cloud Storage) for Prefect Flows.
    Based on deployment type.  Will take in an optional stage id as
    a escape hatch for special circumstances.

    Returns:
        str: Full 3 part stage name.
    """
    return SfGcsStage(
        stage_name=stage_config.dict()[f"{stage_id}_name"],
        stage_location=stage_config.dict()[f"{stage_id}_location"],
    )


@task()
async def query_to_dataframe(
    snowflake_connector: SnowflakeConnector,
    query: str,
    params: Union[tuple[Any], dict[str, Any]] = {},
    poll_frequency_seconds: int = 1,
):
    with snowflake_connector.get_connection() as connection:
        with connection.cursor() as cur:
            response = cur.execute_async(query, params=params)
            query_id = response["queryId"]
            while connection.is_still_running(
                connection.get_query_status_throw_if_error(query_id)
            ):
                await asyncio.sleep(poll_frequency_seconds)
            cur.get_results_from_sfqid(query_id)
            df = cur.fetch_pandas_all()
            return df


@task()
async def query_to_dataframe_batches(
    snowflake_connector: SnowflakeConnector,
    query: str,
    params: Union[tuple[Any], dict[str, Any]] = {},
    poll_frequency_seconds: int = 1,
):
    with snowflake_connector.get_connection() as connection:
        with connection.cursor() as cur:
            response = cur.execute_async(query, params=params)
            query_id = response["queryId"]
            while connection.is_still_running(
                connection.get_query_status_throw_if_error(query_id)
            ):
                await asyncio.sleep(poll_frequency_seconds)
            cur.get_results_from_sfqid(query_id)
            for df in cur.fetch_pandas_batches():
                yield df
