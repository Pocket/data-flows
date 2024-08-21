from unittest.mock import MagicMock

import pendulum.parser
import pytest
from common.databases.snowflake_utils import MozSnowflakeConnector
from dask.distributed import Client
from prefect import flow, task
from recommendation_api.corpus_candidate_sets_flow import (
    CS,
    corpus_candidate_sets,
    create_all_candidate_set_configs,
    load_corpus_candidate_set_records,
    prep_dataframe,
    static_candidate_set_configs,
)
from shared.models.corpus_candidate_set_configs import CorpusCandidateSetConfig

TEST_CONFIG = [
    {
        "CORPUS_CANDIDATE_SET_ID": "dd57c71e-049e-4f3a-b003-9d44d693d8c4",
        "CORPUS_TOPIC_ID": "BUSINESS",
        "NAME": "en_us/business",
        "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_US",
    },
    {
        "CORPUS_CANDIDATE_SET_ID": "c5b8933f-83b5-4867-95af-9a025fe69115",
        "CORPUS_TOPIC_ID": "CAREER",
        "NAME": "en_us/career",
        "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_US",
    },
    {
        "CORPUS_CANDIDATE_SET_ID": "7d5b18b4-417d-47a5-8e55-a53ab7edea7b",
        "CORPUS_TOPIC_ID": "CORONAVIRUS",
        "NAME": "en_us/coronavirus",
        "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_US",
    },
    {
        "CORPUS_CANDIDATE_SET_ID": "30a4f4e6-f4ec-4c8a-99f7-d75c147539ec",
        "CORPUS_TOPIC_ID": "EDUCATION",
        "NAME": "en_us/education",
        "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_US",
    },
]


def test_prep_dataframe():
    sc = CorpusCandidateSetConfig(
        id="test", name="test", query_filename="test.sql", query_params={}
    )
    x = prep_dataframe.fn(sc, [1, 2, 3, 4])
    results = x.to_dict()
    assert results["id"][0] == "test"
    assert results["corpus_items"][0] == "[1, 2, 3, 4]"
    assert pendulum.parser.parse(results["unloaded_at"][0])


@pytest.mark.asyncio
async def test_create_all_candidate_set_configs(monkeypatch):
    state = {"call_count": 0}

    @task()
    async def fake_task(*args, **kwargs):
        state["call_count"] += 1
        return TEST_CONFIG

    monkeypatch.setattr(
        "recommendation_api.corpus_candidate_sets_flow.snowflake_query", fake_task
    )
    x = await create_all_candidate_set_configs(
        static_candidate_set_configs, MagicMock()
    )
    assert state["call_count"] == 1
    assert x == [
        CorpusCandidateSetConfig(
            id="92af3dae-25c9-46c3-bf05-18082aacc7e1",
            name="en_us/collections_by_recency",
            query_filename="collections_by_recency.sql",
            query_params={
                "LANGUAGE": "EN",
                "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_US",
                "MAX_AGE_DAYS": -60,
            },
            is_multiquery=False,
        ),
        CorpusCandidateSetConfig(
            id="ce0e010b-d73d-45e2-a4cd-4abbff74d168",
            name="de_de/collections_by_recency_de_de",
            query_filename="collections_by_recency.sql",
            query_params={
                "LANGUAGE": "DE",
                "SCHEDULED_SURFACE_ID": "NEW_TAB_DE_DE",
                "MAX_AGE_DAYS": -60,
            },
            is_multiquery=False,
        ),
        CorpusCandidateSetConfig(
            id="da9cb7a1-3a34-4211-b918-73819a5586c8",
            name="en_us/life_hacks",
            query_filename="life_hacks.sql",
            query_params={
                "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_US",
                "MAX_AGE_DAYS": 30,
                "CORPUS_TOPIC_LIST": [
                    "SELF_IMPROVEMENT",
                    "CAREER",
                    "HEALTH_FITNESS",
                    "PERSONAL_FINANCE",
                ],
            },
            is_multiquery=False,
        ),
        CorpusCandidateSetConfig(
            id="1b086a7e-7f49-416b-8fd6-254d84001f7c",
            name="de_de/life_hacks",
            query_filename="life_hacks.sql",
            query_params={
                "SCHEDULED_SURFACE_ID": "NEW_TAB_DE_DE",
                "MAX_AGE_DAYS": 30,
                "CORPUS_TOPIC_LIST": [
                    "SELF_IMPROVEMENT",
                    "CAREER",
                    "HEALTH_FITNESS",
                    "PERSONAL_FINANCE",
                ],
            },
            is_multiquery=False,
        ),
        CorpusCandidateSetConfig(
            id="b5179696-4516-4d2f-b42b-b0424e3e4d18",
            name="en_gb/life_hacks",
            query_filename="life_hacks.sql",
            query_params={
                "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_GB",
                "MAX_AGE_DAYS": 30,
                "CORPUS_TOPIC_LIST": [
                    "SELF_IMPROVEMENT",
                    "CAREER",
                    "HEALTH_FITNESS",
                    "PERSONAL_FINANCE",
                ],
            },
            is_multiquery=False,
        ),
        CorpusCandidateSetConfig(
            id="c082fb1f-bec9-45e5-b119-e658cc29366c",
            name="fr_fr/life_hacks",
            query_filename="life_hacks.sql",
            query_params={
                "SCHEDULED_SURFACE_ID": "NEW_TAB_FR_FR",
                "MAX_AGE_DAYS": 30,
                "CORPUS_TOPIC_LIST": [
                    "SELF_IMPROVEMENT",
                    "CAREER",
                    "HEALTH_FITNESS",
                    "PERSONAL_FINANCE",
                ],
            },
            is_multiquery=False,
        ),
        CorpusCandidateSetConfig(
            id="22312367-36a5-4ceb-bce6-7fea7e83759b",
            name="it_it/life_hacks",
            query_filename="life_hacks.sql",
            query_params={
                "SCHEDULED_SURFACE_ID": "NEW_TAB_IT_IT",
                "MAX_AGE_DAYS": 30,
                "CORPUS_TOPIC_LIST": [
                    "SELF_IMPROVEMENT",
                    "CAREER",
                    "HEALTH_FITNESS",
                    "PERSONAL_FINANCE",
                ],
            },
            is_multiquery=False,
        ),
        CorpusCandidateSetConfig(
            id="c62e86b4-8d88-4036-80e3-00394323946f",
            name="es_es/life_hacks",
            query_filename="life_hacks.sql",
            query_params={
                "SCHEDULED_SURFACE_ID": "NEW_TAB_ES_ES",
                "MAX_AGE_DAYS": 30,
                "CORPUS_TOPIC_LIST": [
                    "SELF_IMPROVEMENT",
                    "CAREER",
                    "HEALTH_FITNESS",
                    "PERSONAL_FINANCE",
                ],
            },
            is_multiquery=False,
        ),
        CorpusCandidateSetConfig(
            id="92411893-ebdb-4a43-ad29-aa79e56e2136",
            name="en_us/pocket_hits",
            query_filename="pocket_hits.sql",
            query_params={"SCHEDULED_SURFACE_ID": "POCKET_HITS_EN_US"},
            is_multiquery=False,
        ),
        CorpusCandidateSetConfig(
            id="5f0dae93-a5a8-439a-a2e2-5d418c04bc98",
            name="en_us/new_tab_not_syndicated_or_collection",
            query_filename="scheduled_not_syndicated_or_collection.sql",
            query_params={"MAX_AGE_DAYS": -3, "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_US"},
            is_multiquery=False,
        ),
        CorpusCandidateSetConfig(
            id="92013292-bc4b-4ee1-815a-0e51c5953ff2",
            name="de_de/new_tab_not_syndicated_or_collection",
            query_filename="scheduled_not_syndicated_or_collection.sql",
            query_params={"MAX_AGE_DAYS": -3, "SCHEDULED_SURFACE_ID": "NEW_TAB_DE_DE"},
            is_multiquery=False,
        ),
        CorpusCandidateSetConfig(
            id="43637b16-1572-4f9b-ba5b-cb686d665633",
            name="en_gb/new_tab_not_syndicated_or_collection",
            query_filename="scheduled_not_syndicated_or_collection.sql",
            query_params={"MAX_AGE_DAYS": -3, "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_GB"},
            is_multiquery=False,
        ),
        CorpusCandidateSetConfig(
            id="70c4dffe-dd0b-4d97-ba76-69778b921b21",
            name="fr_fr/new_tab_not_syndicated_or_collection",
            query_filename="scheduled_not_syndicated_or_collection.sql",
            query_params={"MAX_AGE_DAYS": -3, "SCHEDULED_SURFACE_ID": "NEW_TAB_FR_FR"},
            is_multiquery=False,
        ),
        CorpusCandidateSetConfig(
            id="bbf0dccb-d1a2-45bd-a0fd-7d8b8f61bb7a",
            name="it_it/new_tab_not_syndicated_or_collection",
            query_filename="scheduled_not_syndicated_or_collection.sql",
            query_params={"MAX_AGE_DAYS": -3, "SCHEDULED_SURFACE_ID": "NEW_TAB_IT_IT"},
            is_multiquery=False,
        ),
        CorpusCandidateSetConfig(
            id="62b47b84-c32b-4798-8599-e75d61f8c21b",
            name="es_es/new_tab_not_syndicated_or_collection",
            query_filename="scheduled_not_syndicated_or_collection.sql",
            query_params={"MAX_AGE_DAYS": -3, "SCHEDULED_SURFACE_ID": "NEW_TAB_IT_IT"},
            is_multiquery=False,
        ),
        CorpusCandidateSetConfig(
            id="2066c835-a940-45ec-b1f7-267457d9e0a2",
            name="en_us/recommended_by_recency",
            query_filename="recommended_by_recency.sql",
            query_params={
                "N_RECS_PER_TOPIC": 6,
                "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_US",
            },
            is_multiquery=True,
        ),
        CorpusCandidateSetConfig(
            id="0e0a8663-a2c1-430e-9b8c-3a5b6d9eda11",
            name="en_us/new_tab_syndicated",
            query_filename="scheduled_syndicated.sql",
            query_params={"MAX_AGE_DAYS": -30, "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_US"},
            is_multiquery=False,
        ),
        CorpusCandidateSetConfig(
            id="33cd5817-cbc4-4d01-b9be-7dbcf4e26d2e",
            name="en_us/collections_pride",
            query_filename="labeled_collection_stories.sql",
            query_params={"COLLECTION_LABEL": "pride", "LANGUAGE": "EN"},
            is_multiquery=False,
        ),
        CorpusCandidateSetConfig(
            id="8c4f7d7e-f0e0-4c99-9ebb-c889b78dfd66",
            name="de_de/collections_pride",
            query_filename="labeled_collection_stories.sql",
            query_params={"COLLECTION_LABEL": "pride", "LANGUAGE": "DE"},
            is_multiquery=False,
        ),
        CorpusCandidateSetConfig(
            id="dd57c71e-049e-4f3a-b003-9d44d693d8c4",
            name="en_us/business",
            query_filename="topic.sql",
            query_params={
                "CORPUS_TOPIC_ID": "BUSINESS",
                "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_US",
            },
            is_multiquery=False,
        ),
        CorpusCandidateSetConfig(
            id="c5b8933f-83b5-4867-95af-9a025fe69115",
            name="en_us/career",
            query_filename="topic.sql",
            query_params={
                "CORPUS_TOPIC_ID": "CAREER",
                "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_US",
            },
            is_multiquery=False,
        ),
        CorpusCandidateSetConfig(
            id="7d5b18b4-417d-47a5-8e55-a53ab7edea7b",
            name="en_us/coronavirus",
            query_filename="topic.sql",
            query_params={
                "CORPUS_TOPIC_ID": "CORONAVIRUS",
                "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_US",
            },
            is_multiquery=False,
        ),
        CorpusCandidateSetConfig(
            id="30a4f4e6-f4ec-4c8a-99f7-d75c147539ec",
            name="en_us/education",
            query_filename="topic.sql",
            query_params={
                "CORPUS_TOPIC_ID": "EDUCATION",
                "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_US",
            },
            is_multiquery=False,
        ),
    ]


@pytest.mark.asyncio
async def test_load_corpus_candidate_set_records(monkeypatch):
    state = {"t_call_count": 0, "f_call_count": 0, "m_call_count": 0}

    @task()
    async def fake_task(*args, **kwargs):
        state["t_call_count"] += 1
        return [{"ID": "test", "TOPIC": "test", "PUBLISHER": "test"}]

    @task()
    async def fake_task_multi(*args, **kwargs):
        state["m_call_count"] += 1
        return [[{"ID": "test", "TOPIC": "test", "PUBLISHER": "test"}]]

    @flow()
    async def fake_flow(dataframe, feature_group_name, *args, **kwargs):
        environment_map = {"dev": "development", "production": "production"}

        state["f_call_count"] += 1
        assert (
            feature_group_name
            == f"{environment_map[CS.dev_or_production]}-corpus-candidate-sets-v1"
        )
        return True

    monkeypatch.setattr(
        "recommendation_api.corpus_candidate_sets_flow.snowflake_query", fake_task
    )

    monkeypatch.setattr(
        "recommendation_api.corpus_candidate_sets_flow.snowflake_multiquery",
        fake_task_multi,
    )

    monkeypatch.setattr(
        "recommendation_api.corpus_candidate_sets_flow.dataframe_to_feature_group",
        fake_flow,
    )

    test_config = CorpusCandidateSetConfig(
        id="30a4f4e6-f4ec-4c8a-99f7-d75c147539ec",
        name="en_us/education",
        query_filename="topic.sql",
        query_params={
            "CORPUS_TOPIC_ID": "EDUCATION",
            "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_US",
        },
    )

    multi_test_config = CorpusCandidateSetConfig(
        id="2066c835-a940-45ec-b1f7-267457d9e0a2",
        name="en_us/recommended_by_recency",
        query_filename="recommended_by_recency.sql",
        query_params={
            "N_RECS_PER_TOPIC": 6,
            "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_US",
        },
        is_multiquery=True,
    )

    dask_client = Client(processes=False)
    sfc = MozSnowflakeConnector()

    await load_corpus_candidate_set_records(test_config, sfc, dask_client)  # type: ignore
    assert state["t_call_count"] == 1
    assert state["f_call_count"] == 1
    assert state["m_call_count"] == 0

    await load_corpus_candidate_set_records(multi_test_config, sfc, dask_client)  # type: ignore
    assert state["t_call_count"] == 1
    assert state["m_call_count"] == 1


@pytest.mark.asyncio
async def test_corpus_candidate_sets(monkeypatch):
    state = {"sf_call_count": 0, "load_call_count": 0}

    @task()
    async def fake_task(*args, **kwargs):
        state["sf_call_count"] += 1
        return TEST_CONFIG

    @flow()
    async def fake_flow(*args, **kwargs):
        state["load_call_count"] += 1
        return True

    monkeypatch.setattr(
        "recommendation_api.corpus_candidate_sets_flow.snowflake_query", fake_task
    )

    monkeypatch.setattr(
        "recommendation_api.corpus_candidate_sets_flow.load_corpus_candidate_set_records",
        fake_flow,
    )

    await corpus_candidate_sets()
    assert state["sf_call_count"] == 1
    assert state["load_call_count"] == 23
