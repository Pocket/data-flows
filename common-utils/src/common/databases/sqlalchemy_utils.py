from common.settings import NestedSettings, SecretSettings, get_cached_settings
from prefect_sqlalchemy import DatabaseCredentials


class SqlalchemyCredSettings(NestedSettings):
    """Settings that can be passed in to
    Snowflake Credentials.  This is mean to be used as
    a sub model of the SqlalchemySettings model.
    """

    url: str
    read_url: str


class SqlalchemySettings(SecretSettings):
    """Settings that can be passed in for
    Sqlalchemy connection.
    """

    sqlalchemy_credentials: SqlalchemyCredSettings


class MozSqlalchemyCredentials(DatabaseCredentials):
    """Mozilla version of the Sqlalchemy DatabaseCredentials provided
    by Prefect-Sqlalchemy with settings already applied.
    All other base model attributes can be set explicitly here.

    See https://prefecthq.github.io/prefect-sqlalchemy/ for usage.
    """

    def __init__(self, **data):
        """Set credentials and other settings on usage of
        model.
        """
        settings = get_cached_settings(SqlalchemySettings)
        data["url"] = settings.sqlalchemy_credentials.url
        super().__init__(**data)


class MozSqlalchemyCredentialsRead(DatabaseCredentials):
    """Mozilla version of the Sqlalchemy DatabaseCredentials provided
    by Prefect-Sqlalchemy with settings already applied.
    All other base model attributes can be set explicitly here.
    Used for extracting to S3

    See https://prefecthq.github.io/prefect-sqlalchemy/ for usage.
    """

    def __init__(self, **data):
        """Set credentials and other settings on usage of
        model.
        """
        settings = get_cached_settings(SqlalchemySettings)
        data["url"] = settings.sqlalchemy_credentials.read_url
        super().__init__(**data)
