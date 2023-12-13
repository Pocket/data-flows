import os

from boto3 import Session
from common.cloud.aws_utils import fetch_aws_secret
from common.settings import SecretSettings
from moto import mock_secretsmanager


def test_fetch_aws_secret():
    with mock_secretsmanager():

        class Settings(SecretSettings):
            test_secret: str

        sm = Session().client(
            service_name="secretsmanager",
        )
        sm.create_secret(Name="data-flows/dev/test-secret", SecretString="test")
        x = fetch_aws_secret(Settings)  # type: ignore
        assert x == {"test_secret": "test"}


def test_fetch_aws_secret_explicit():
    with mock_secretsmanager():
        os.environ["DR_CONFIG_TEST_ENV"] = "testing"

        class Settings(SecretSettings):
            test_secret: str
            test_env: str

            class Config:
                secret_fields = ["test_secret"]

        sm = Session().client(
            service_name="secretsmanager",
        )
        sm.create_secret(Name="data-flows/dev/test-secret", SecretString="test")
        x = fetch_aws_secret(Settings)  # type: ignore
        assert x == {"test_secret": "test"}
