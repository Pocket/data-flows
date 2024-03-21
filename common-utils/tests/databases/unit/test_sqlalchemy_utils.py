from common.databases.sqlalchemy_utils import (
    MozSqlalchemyCredentials,
    MozSqlalchemyCredentialsRead,
)


def test_pkt_sqlalchemy_credentials():
    x = MozSqlalchemyCredentials()
    assert x.url == "mysql://scott:tiger@localhost:5432"


def test_pkt_sqlalchemy_credentials_read():
    x = MozSqlalchemyCredentialsRead()
    assert x.url == "mysql://scott:tiger@localhost:5432"
