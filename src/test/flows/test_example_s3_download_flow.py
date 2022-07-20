from unittest.mock import MagicMock

import pytest

pytest.importorskip("boto3")

from flows.examples.s3_download_flow import flow


@pytest.fixture
def mocked_boto_client(monkeypatch):
    boto3 = MagicMock()
    client = boto3.session.Session().client()
    boto3.client = MagicMock(return_value=client)
    monkeypatch.setattr("prefect.utilities.aws.boto3", boto3)
    return client


class TestExampleS3Download:
    def test_flow_run(self, mocked_boto_client):
        state = flow.run()
        assert state.is_successful()
