import pytest
from candidate_sets.candidate_config import GET_TOPICS_SQL
from candidate_sets.legacy_candidate_sets_flow import create_legacy_candidate_set
from prefect import task

TEST_DATA = {
    "longreads": [{"ID": 0, "PUBLISHER": "test.com"}],
    "curated_feeds": [
        {"ID": 0, "PUBLISHER": "test.com", "IS_SYNDICATED": True, "IS_COLLECTION": True}
    ],
    "shortreads": [{"ID": 0, "PUBLISHER": "test.com"}],
    "syndicated_feed": [{"ID": 0, "PUBLISHER": "test.com"}],
    "topics": [{"ID": 0, "PUBLISHER": "test.com"}],
}


@pytest.mark.parametrize(
    "param_id",
    ["topics", "longreads", "shortreads", "syndicated_feed", "curated_feeds"],
)
@pytest.mark.asyncio
async def test_flow(param_id, monkeypatch):
    state = {"sf_call_count": 0, "sqs_call_count": 0}

    sf_call_count = 1
    sqs_call_count = 2
    if param_id in ["topics", "longreads", "shortreads"]:
        sf_call_count = 2
    if param_id == "curated_feeds":
        sf_call_count = 4

    @task()
    async def fake_task(*args, **kwargs):
        state["sf_call_count"] += 1
        if kwargs["query"] == GET_TOPICS_SQL:
            return [
                {
                    "LEGACY_CURATED_CORPUS_CANDIDATE_SET_ID": "0fed312e-c3ac-499a-8d3b-064b254e5cce",  # noqa: E501
                    "CORPUS_TOPIC_ID": "0fed312e-c3ac-499a-8d3b-064b254e5cce",
                }
            ]
        return TEST_DATA[param_id]

    @task()
    def fake_sqs(*args, **kwargs):
        state["sqs_call_count"] += 1

    monkeypatch.setattr(
        "candidate_sets.legacy_candidate_sets_flow.snowflake_query", fake_task
    )
    monkeypatch.setattr(
        "candidate_sets.legacy_candidate_sets_flow.put_results", fake_sqs
    )
    await create_legacy_candidate_set(param_id)
    assert state["sf_call_count"] == sf_call_count
    assert state["sqs_call_count"] == sqs_call_count
