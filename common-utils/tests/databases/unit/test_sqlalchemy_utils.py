from common.databases.sqlalchemy_utils import MozSqlalchemyCredentials


def test_pkt_sqlalchemy_credentials():
    x = MozSqlalchemyCredentials()
    assert x.url == "mysql://scott:tiger@localhost:5432"
