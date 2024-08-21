from typing import Dict, Optional

from pydantic import BaseModel, Field


class CorpusCandidateSetConfig(BaseModel):
    id: str = Field(description="UUID identifying the candidate set")
    name: str = Field(description="Internal human-readable name for the candidate set")
    query_filename: str = Field(
        description="Filename of Snowflake query located in src/flows/recommendation_api/corpus_candidate_sets/sql/. "
        'Query result must contain CorpusItem columns "ID", "TOPIC", "PUBLISHER".'
    )
    query_params: Optional[Dict] = Field(
        description="Optional Snowflake query parameters"
    )
    is_multiquery: bool = False


static_candidate_set_configs = [
    CorpusCandidateSetConfig(
        id="92af3dae-25c9-46c3-bf05-18082aacc7e1",
        name="en_us/collections_by_recency",
        query_filename="collections_by_recency.sql",
        query_params={
            "LANGUAGE": "EN",
            "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_US",
            "MAX_AGE_DAYS": -60,
        },
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
    ),
    CorpusCandidateSetConfig(
        id="da9cb7a1-3a34-4211-b918-73819a5586c8",
        name="en_us/life_hacks",
        query_filename="life_hacks.sql",
        query_params={
            "MAX_AGE_DAYS": 30,
            "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_US",
            "CORPUS_TOPIC_LIST": [
                "SELF_IMPROVEMENT",
                "CAREER",
                "HEALTH_FITNESS",
                "PERSONAL_FINANCE",
            ],
        },
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
    ),
    CorpusCandidateSetConfig(
        id="92411893-ebdb-4a43-ad29-aa79e56e2136",
        name="en_us/pocket_hits",
        query_filename="pocket_hits.sql",
        query_params={"SCHEDULED_SURFACE_ID": "POCKET_HITS_EN_US"},
    ),
    CorpusCandidateSetConfig(
        id="5f0dae93-a5a8-439a-a2e2-5d418c04bc98",
        name="en_us/new_tab_not_syndicated_or_collection",
        query_filename="scheduled_not_syndicated_or_collection.sql",
        query_params={"MAX_AGE_DAYS": -3, "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_US"},
    ),
    CorpusCandidateSetConfig(
        id="92013292-bc4b-4ee1-815a-0e51c5953ff2",
        name="de_de/new_tab_not_syndicated_or_collection",
        query_filename="scheduled_not_syndicated_or_collection.sql",
        query_params={"MAX_AGE_DAYS": -3, "SCHEDULED_SURFACE_ID": "NEW_TAB_DE_DE"},
    ),
    CorpusCandidateSetConfig(
        id="43637b16-1572-4f9b-ba5b-cb686d665633",
        name="en_gb/new_tab_not_syndicated_or_collection",
        query_filename="scheduled_not_syndicated_or_collection.sql",
        query_params={"MAX_AGE_DAYS": -3, "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_GB"},
    ),
    CorpusCandidateSetConfig(
        id="70c4dffe-dd0b-4d97-ba76-69778b921b21",
        name="fr_fr/new_tab_not_syndicated_or_collection",
        query_filename="scheduled_not_syndicated_or_collection.sql",
        query_params={"MAX_AGE_DAYS": -3, "SCHEDULED_SURFACE_ID": "NEW_TAB_FR_FR"},
    ),
    CorpusCandidateSetConfig(
        id="bbf0dccb-d1a2-45bd-a0fd-7d8b8f61bb7a",
        name="it_it/new_tab_not_syndicated_or_collection",
        query_filename="scheduled_not_syndicated_or_collection.sql",
        query_params={"MAX_AGE_DAYS": -3, "SCHEDULED_SURFACE_ID": "NEW_TAB_IT_IT"},
    ),
    CorpusCandidateSetConfig(
        id="62b47b84-c32b-4798-8599-e75d61f8c21b",
        name="es_es/new_tab_not_syndicated_or_collection",
        query_filename="scheduled_not_syndicated_or_collection.sql",
        query_params={"MAX_AGE_DAYS": -3, "SCHEDULED_SURFACE_ID": "NEW_TAB_IT_IT"},
    ),
    CorpusCandidateSetConfig(
        id="2066c835-a940-45ec-b1f7-267457d9e0a2",
        name="en_us/recommended_by_recency",
        query_filename="recommended_by_recency.sql",
        query_params={"N_RECS_PER_TOPIC": 6, "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_US"},
        is_multiquery=True,
    ),
    CorpusCandidateSetConfig(
        id="0e0a8663-a2c1-430e-9b8c-3a5b6d9eda11",
        name="en_us/new_tab_syndicated",
        query_filename="scheduled_syndicated.sql",
        query_params={"MAX_AGE_DAYS": -30, "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_US"},
    ),
    CorpusCandidateSetConfig(
        id="33cd5817-cbc4-4d01-b9be-7dbcf4e26d2e",
        name="en_us/collections_pride",
        query_filename="labeled_collection_stories.sql",
        query_params={"COLLECTION_LABEL": "pride", "LANGUAGE": "EN"},
    ),
    CorpusCandidateSetConfig(
        id="8c4f7d7e-f0e0-4c99-9ebb-c889b78dfd66",
        name="de_de/collections_pride",
        query_filename="labeled_collection_stories.sql",
        query_params={"COLLECTION_LABEL": "pride", "LANGUAGE": "DE"},
    ),
]
