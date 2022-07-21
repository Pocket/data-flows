import json
from typing import Dict, List
from prefect import Flow, unmapped, task

from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from common_tasks.corpus_candidate_set import (
    create_corpus_candidate_set_record,
    load_feature_record,
    feature_group,
    validate_corpus_items,
)
from utils import config
from utils.flow import get_flow_name, get_interval_schedule

FLOW_NAME = get_flow_name(__file__)


@task
def load_topic_slate_query() -> str:
    """
    returns a query template for the most recently scheduled curated
    items by topic.  requires setting parameters to execute
    """
    with open("curated_topic_slate.sql", "r") as fp:
        topic_query = fp.read()
    return topic_query

@task
def load_topic_slate_guids() -> List[Dict]:
    """
    returns a list of dicts with per-topic metadata including
    candidate set GUIDs for recommendation-api slates/lineups
    """
    with open("candidate_guid_map.json", "r") as fp:
        topic_guids = json.load(fp)
    return topic_guids

@task
def create_query_parameters(topic_ids: List) -> List[Dict]:
    """
    returns a list of dicts with per-topic parameters to be
    supplied to the query template
    """
    return [{"TOPIC": topic_info["curatorLabel"],
             "NUM_CANDIDATES": 45,
             "SCHEDULED_SURFACE_ID": "NEW_TAB_EN_US"
             }
            for topic_info in topic_ids]

@task
def get_candidate_set_guids(topic_ids: List) -> List[str]:
    """
    returns a list of candidate set guids ordered consistently
    with the input list of per-topic metadata
    """
    return [topic_info["curatedCandidateID"]
            for topic_info in topic_ids]


# with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=30)) as flow:
with Flow(FLOW_NAME) as flow:

    # load GUIDs for each topic's curated candidate set
    topic_guids = load_topic_slate_guids()

    # load per-topic snowflake query template
    topic_query = load_topic_slate_query()

    # create per-topic query parameters for mapping query task
    query_params = create_query_parameters(topic_guids)

    # perform snowflake queries for each topic
    corpus_items = PocketSnowflakeQuery().map(query=unmapped(topic_query),
                                              data=query_params,
                                              database=unmapped(config.SNOWFLAKE_ANALYTICS_DATABASE),
                                              schema=unmapped(config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA),
                                              output_type=unmapped(OutputType.DICT) )

    # validate query results for each topic
    corpus_items = validate_corpus_items.map(corpus_items)

    # make list of curated candidate set GUIDs for each topic
    curated_candidate_set_ids = get_candidate_set_guids(topic_guids)

    # create feature group records for per-topic curated candidate sets
    feature_group_records = create_corpus_candidate_set_record.map(id=curated_candidate_set_ids,
                                                                   corpus_items=corpus_items)

    # load curated candidate sets for each topic into feature group
    load_feature_record.map(record=feature_group_records,
                            feature_group_name=unmapped(feature_group))

if __name__ == "__main__":

    state = flow.run()

