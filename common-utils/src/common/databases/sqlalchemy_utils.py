from common.settings import CommonSettings, Settings
from prefect_sqlalchemy import DatabaseCredentials

CS = CommonSettings()  # type: ignore


class SqlalchemySettings(Settings):
    """Settings that can be passed in for
    Sqlalchemy Credentials.
    """

    sqlalchemy_url: str


class MzsSqlalchemy(DatabaseCredentials):
    """Moz Social version of the Sqlalchemy DatabaseCredentials provided
    by Prefect-Sqlalchemy with settings already applied.
    All other base model attributes can be set explicitly here.

    See https://prefecthq.github.io/prefect-sqlalchemy/ for usage.
    """

    def __init__(self, **data):
        """Set credentials and other settings on usage of
        model.
        """
        settings = SqlalchemySettings()  # type: ignore
        data["url"] = settings.sqlalchemy_url
        super().__init__(**data)
