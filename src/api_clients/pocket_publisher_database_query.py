import os
from typing import Any

from prefect.tasks.mysql import MySQLFetch
from prefect.utilities.tasks import defaults_from_attrs


class PocketPublisherDatabaseQuery(MySQLFetch):
    """
    PocketPublisherDatabaseQuery automatically connects to our Publisher MySQL Database if the right environment variables are set:
    - POCKET_PUBLISHER_DATABASE_HOST
    - POCKET_PUBLISHER_DATABASE_PORT
    - POCKET_PUBLISHER_DATABASE_DBNAME
    - POCKET_PUBLISHER_DATABASE_USER
    - POCKET_PUBLISHER_DATABASE_PASSWORD
    """

    def __init__(
            self,
            query: str = None,
            database_host_env_var_name: str = "POCKET_PUBLISHER_DATABASE_HOST",
            database_port_env_var_name: str = "POCKET_PUBLISHER_DATABASE_PORT",
            database_dbname_env_var_name: str = "POCKET_PUBLISHER_DATABASE_DBNAME",
            database_user_var_name: str = "POCKET_PUBLISHER_DATABASE_USER",
            database_password_var_name: str = "POCKET_PUBLISHER_DATABASE_PASSWORD",
            **kwargs: Any,  # See the base SnowflakeQuery class for additional arguments
    ):
        super().__init__(
            query=query,
            **kwargs
        )
        self.database_dbname_env_var_name = database_dbname_env_var_name
        self.database_user_var_name = database_user_var_name
        self.database_host_env_var_name = database_host_env_var_name
        self.database_port_env_var_name = database_port_env_var_name
        self.database_password_var_name = database_password_var_name

    @defaults_from_attrs(
        "query",
        "database_dbname_env_var_name",
        "database_user_var_name",
        "database_host_env_var_name",
        "database_port_env_var_name",
        "database_password_var_name",
    )
    def run(
            self,
            query: str = None,
            database_dbname_env_var_name: str = None,
            database_dbname: str = None,
            database_user_var_name: str = None,
            database_user: str = None,
            database_host_env_var_name: str = None,
            database_host: str = None,
            database_port_env_var_name: str = None,
            database_port: int = None,
            database_password_var_name: str = None,
            database_password: str = None,
            **kwargs):

        if database_dbname is None and database_dbname_env_var_name in os.environ:
            database_dbname = os.environ.get(database_dbname_env_var_name)

        if database_user is None and database_user_var_name in os.environ:
            database_user = os.environ.get(database_user_var_name)

        if database_host is None and database_host_env_var_name in os.environ:
            database_host = os.environ.get(database_host_env_var_name)

        if database_port is None and database_port_env_var_name in os.environ:
            database_port = int(os.environ.get(database_port_env_var_name))

        if database_password is None and database_password_var_name in os.environ:
            database_password = os.environ.get(database_password_var_name)

        self.logger.info(f"Connecting using database={kwargs.get('database')} and schema={kwargs.get('schema')}."
                         f" (If 'None' then the default database/schema is used.)")
        self.logger.info(query)

        query_result = super().run(
            query=query,
            host=database_host,
            port=database_port,
            user=database_user,
            password=database_password,
            db_name=database_dbname,
            **kwargs
        )

        self.logger.info(f'Row Count: {len(query_result)}')

        return query_result
