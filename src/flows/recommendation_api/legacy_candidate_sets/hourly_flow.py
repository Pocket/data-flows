from prefect import Flow, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from flows.recommendation_api.legacy_candidate_sets import curated_feeds, longreads, shortreads, syndicated_feed, topics
from utils import config
from utils.flow import get_flow_name

FLOW_NAME = get_flow_name(__file__)

FLOW_NAMES = [
    curated_feeds.FLOW_NAME,
    longreads.FLOW_NAME,
    shortreads.FLOW_NAME,
    syndicated_feed.FLOW_NAME,
    topics.FLOW_NAME
]

with Flow(FLOW_NAME, executor=LocalDaskExecutor()) as flow:
    ids = create_flow_run.map(flow_name=FLOW_NAMES, project_name=unmapped(config.PREFECT_PROJECT_NAME))
    wait_for_flow_run.map(ids)

if __name__ == "__main__":
    flow.run()
