from prefect import Flow, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from flows.recommendation_api.corpus_candidate_sets import collections_by_recency, lifehacks, new_tab_not_syndicated, \
    pockethits, recommended_by_recency, setup_moment_editorial_selection, setup_moment_scheduled_syndicated, topics
from utils import config
from utils.flow import get_flow_name

FLOW_NAME = get_flow_name(__file__)

FLOW_NAMES = [
    collections_by_recency.FLOW_NAME,
    lifehacks.FLOW_NAME,
    new_tab_not_syndicated.FLOW_NAME,
    pockethits.FLOW_NAME,
    recommended_by_recency.FLOW_NAME,
    setup_moment_editorial_selection.FLOW_NAME,
    setup_moment_scheduled_syndicated.FLOW_NAME,
    topics.FLOW_NAME,
]

with Flow(FLOW_NAME, executor=LocalDaskExecutor()) as flow:
    ids = create_flow_run.map(flow_name=FLOW_NAMES, project_name=unmapped(config.PREFECT_PROJECT_NAME))
    wait_for_flow_run.map(ids)

if __name__ == "__main__":
    flow.run()
