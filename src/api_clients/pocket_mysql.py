import logging
from typing import Union

import pymysql
from prefect.tasks.mysql import MySQLExecute
from prefect.utilities.tasks import defaults_from_attrs


class PocketMySQLExecuteMany(MySQLExecute):
    @defaults_from_attrs(
        "db_name",
        "user",
        "password",
        "host",
        "port",
        "query",
        "charset",
        "ssl",
    )
    def run(
            self,
            db_name: str = None,
            user: str = None,
            password: str = None,
            host: str = None,
            port: int = None,
            query: str = None,
            args: Union[tuple, list, dict] = None,
            commit: bool = True,
            charset: str = None,
            ssl: dict = None,
    ) -> int:
        """
        Task run method. Executes a query against MySQL database.

        Args:
            - db_name (str): name of MySQL database
            - user (str): user name used to authenticate
            - password (str): password used to authenticate
            - host (str): database host address
            - port (int, optional): port used to connect to MySQL database, defaults to 3307
                if not provided
            - query (str, optional): query to execute against database
            - commit (bool, optional): set to True to commit transaction, defaults to false
            - charset (str, optional): charset you want to use (defaults to "utf8mb4")
            - ssl (dict, optional): A dict of arguments similar to mysql_ssl_set()’s
                parameters used for establishing encrypted connections using SSL. To connect
                with SSL, at least `ssl_ca`, `ssl_cert`, and `ssl_key` must be specified.

        Returns:
            - executed (int): number of affected rows

        Raises:
            - pymysql.MySQLError
        """
        if not query:
            raise ValueError("A query string must be provided")

        if not args:
            raise ValueError("Args must be provided")

        conn = pymysql.connect(
            host=host,
            user=user,
            password=password,
            db=db_name,
            charset=charset,
            port=port,
            ssl=ssl,
        )

        try:
            with conn.cursor() as cursor:
                executed = cursor.executemany(query, args)
                if commit:
                    conn.commit()

            conn.close()
            logging.debug("Execute Results: %s", executed)
            return executed

        except (Exception, pymysql.MySQLError) as e:
            conn.close()
            logging.debug("Execute Error: %s", e)
            raise e

