from prefect import Flow, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from flows import dbt_hourly_flow, parser_item_html_text_streaming_flow
from flows.braze import update_flow as braze_update_flow
from flows.prospecting import prereview_engagement_feature_store_flow, postreview_engagement_feature_store_flow
from flows.recommendation_api.corpus_candidate_sets import hourly_flow as corpus_candidate_sets_hourly_flow
from flows.recommendation_api.legacy_candidate_sets import hourly_flow as legacy_candidate_sets_hourly_flow
from utils import config
from utils.flow import get_flow_name, get_interval_schedule

FLOW_NAME = get_flow_name(__file__)

EXTRACT_FLOWS = [
    parser_item_html_text_streaming_flow.FLOW_NAME
]

TRANSFORM_FLOWS = [
    dbt_hourly_flow.FLOW_NAME,
]

LOAD_FLOWS = [
    braze_update_flow.FLOW_NAME,
    prereview_engagement_feature_store_flow.FLOW_NAME,
    postreview_engagement_feature_store_flow.FLOW_NAME,
    corpus_candidate_sets_hourly_flow.FLOW_NAME,
    legacy_candidate_sets_hourly_flow.FLOW_NAME,
]

with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=60), executor=LocalDaskExecutor()) as flow:
    project_name = unmapped(config.PREFECT_PROJECT_NAME);

    extract_ids = create_flow_run.map(flow_name=EXTRACT_FLOWS, project_name=project_name)
    extract_complete = wait_for_flow_run.map(extract_ids)

    transform_ids = create_flow_run.map(flow_name=TRANSFORM_FLOWS, project_name=project_name).set_upstream(extract_complete)
    transform_complete = wait_for_flow_run.map(transform_ids)

    load_ids = create_flow_run.map(flow_name=LOAD_FLOWS, project_name=project_name).set_upstream(transform_complete)
    load_complete = wait_for_flow_run.map(load_ids)

if __name__ == "__main__":
    flow.run()
