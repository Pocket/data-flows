from common.databases.sqlalchemy_utils import MzsSqlalchemyCredentials


def test_pkt_sqlalchemy_credentials():
    x = MzsSqlalchemyCredentials()
    assert x.url == "mysql://scott:tiger@localhost:5432"
