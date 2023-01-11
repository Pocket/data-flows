from typing import Dict

from pydantic import BaseModel


class CorpusCandidateSetConfig(BaseModel):
    id: str
    name: str
    query_filename: str  # Query result is expected to have columns 'ID', 'TOPIC', 'PUBLISHER'
    query_params: Dict


corpus_candidate_set_configs = [
    CorpusCandidateSetConfig(
        id='92af3dae-25c9-46c3-bf05-18082aacc7e1',
        name='collections_by_recency_en_us',
        query_filename='collections_by_recency.sql',
        query_params={'LANGUAGE': 'EN', 'SURFACE': 'NEW_TAB_EN_US', 'MAX_AGE_DAYS': -60}
    ),
    CorpusCandidateSetConfig(
        id='ce0e010b-d73d-45e2-a4cd-4abbff74d168',
        name='collections_by_recency_de_de',
        query_filename='collections_by_recency.sql',
        query_params={'LANGUAGE': 'DE', 'SURFACE': 'NEW_TAB_DE_DE', 'MAX_AGE_DAYS': -60},
    ),
    CorpusCandidateSetConfig(
        id='da9cb7a1-3a34-4211-b918-73819a5586c8',
        name='life_hacks_en_us',
        query_filename='life_hacks.sql',
        query_params={
            "MAX_AGE_DAYS": 30,
            "CORPUS_TOPIC_LIST": ['SELF_IMPROVEMENT','CAREER','HEALTH_FITNESS','PERSONAL_FINANCE'],
        },
    ),
    CorpusCandidateSetConfig(
        id='92411893-ebdb-4a43-ad29-aa79e56e2136',
        name='pocket_hits_en_us',
        query_filename='pocket_hits.sql',
        query_params={"SURFACE_GUID": "POCKET_HITS_EN_US"},
    ),
    CorpusCandidateSetConfig(
        id='5f0dae93-a5a8-439a-a2e2-5d418c04bc98',
        name='en_us/new_tab_not_syndicated_or_collection',
        query_filename='scheduled_not_syndicated_or_collection.sql',
        query_params={"MAX_AGE_DAYS": -3, "SURFACE_GUID": "NEW_TAB_EN_US"},
    ),
    CorpusCandidateSetConfig(
        id='2066c835-a940-45ec-b1f7-267457d9e0a2',
        name='en_us/recommended_by_recency',
        query_filename='recommended_by_recency.sql',
        query_params={"N_RECS_PER_TOPIC": 6},
    ),
]
