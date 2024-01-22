import boto3
import pytest
from moto import mock_sqs
from prefect import flow
from shared.api_clients.sqs import RecommendationCandidate, put_results

TEST_DATA = [RecommendationCandidate(feed_id=1, publisher="test", item_id=1)]


@mock_sqs
@pytest.mark.parametrize("test_config", [(False, 1), (True, 0)])
def test_sqs(test_config):
    curated, expiration = test_config

    sqs = boto3.client("sqs")
    sqs.create_queue(
        QueueName="RecommendationAPI-Dev-Sqs-Translation-Queue",
    )

    @flow()
    def test_flow(*args, **kwargs):
        put_results(
            candidate_set_id="0fed312e-c3ac-499a-8d3b-064b254e5cce",
            candidates=TEST_DATA,
            curated=curated,
            expires_in_s=expiration,
        )

    if expiration == 0:
        with pytest.raises(AssertionError):
            test_flow()
    else:
        test_flow()
