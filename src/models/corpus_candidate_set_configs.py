from typing import Dict, Optional

from pydantic import BaseModel, Field


class CorpusCandidateSetConfig(BaseModel):
    id: str = Field(description='UUID identifying the candidate set')
    name: str = Field(description='Internal human-readable name for the candidate set')
    query_filename: str = Field(
        description='Filename of Snowflake query located in src/flows/recommendation_api/corpus_candidate_sets/sql/. '
                    'Query result must contain CorpusItem columns "ID", "TOPIC", "PUBLISHER".')
    query_params: Optional[Dict] = Field(description='Optional Snowflake query parameters')


static_candidate_set_configs = [
    CorpusCandidateSetConfig(
        id='92af3dae-25c9-46c3-bf05-18082aacc7e1',
        name='en_us/collections_by_recency',
        query_filename='collections_by_recency.sql',
        query_params={'LANGUAGE': 'EN', 'SURFACE': 'NEW_TAB_EN_US', 'MAX_AGE_DAYS': -60}
    ),
    CorpusCandidateSetConfig(
        id='ce0e010b-d73d-45e2-a4cd-4abbff74d168',
        name='de_de/collections_by_recency_de_de',
        query_filename='collections_by_recency.sql',
        query_params={'LANGUAGE': 'DE', 'SURFACE': 'NEW_TAB_DE_DE', 'MAX_AGE_DAYS': -60},
    ),
    CorpusCandidateSetConfig(
        id='da9cb7a1-3a34-4211-b918-73819a5586c8',
        name='en_us/life_hacks',
        query_filename='life_hacks.sql',
        query_params={
            'MAX_AGE_DAYS': 30,
            'CORPUS_TOPIC_LIST': ['SELF_IMPROVEMENT','CAREER','HEALTH_FITNESS','PERSONAL_FINANCE'],
        },
    ),
    CorpusCandidateSetConfig(
        id='92411893-ebdb-4a43-ad29-aa79e56e2136',
        name='en_us/pocket_hits',
        query_filename='pocket_hits.sql',
        query_params={'SCHEDULED_SURFACE_ID': 'POCKET_HITS_EN_US'},
    ),
    CorpusCandidateSetConfig(
        id='5f0dae93-a5a8-439a-a2e2-5d418c04bc98',
        name='en_us/new_tab_not_syndicated_or_collection',
        query_filename='scheduled_not_syndicated_or_collection.sql',
        query_params={'MAX_AGE_DAYS': -3, 'SCHEDULED_SURFACE_ID': 'NEW_TAB_EN_US'},
    ),
    CorpusCandidateSetConfig(
        id='92013292-bc4b-4ee1-815a-0e51c5953ff2',
        name='de_de/new_tab_not_syndicated_or_collection',
        query_filename='scheduled_not_syndicated_or_collection.sql',
        query_params={'MAX_AGE_DAYS': -3, 'SCHEDULED_SURFACE_ID': 'NEW_TAB_DE_DE'},
    ),
    CorpusCandidateSetConfig(
        id='2066c835-a940-45ec-b1f7-267457d9e0a2',
        name='en_us/recommended_by_recency',
        query_filename='recommended_by_recency.sql',
        query_params={'N_RECS_PER_TOPIC': 6, 'SCHEDULED_SURFACE_ID': 'NEW_TAB_EN_US'},
    ),
]
