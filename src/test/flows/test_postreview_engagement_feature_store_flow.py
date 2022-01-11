from prefect.core.edge import Edge

from src.flows.prereview_engagement_feature_store_flow import *

assert promised_get_last_executed_flow_result in flow.tasks
assert promised_update_last_executed_flow_result in flow.tasks
assert promised_extract_from_snowflake_result in flow.tasks
assert promised_dataframe_to_feature_group_result in flow.tasks
assert len(flow.tasks) == 4

assert flow.root_tasks() == { promised_get_last_executed_flow_result, promised_update_last_executed_flow_result }
assert flow.terminal_tasks() == { promised_update_last_executed_flow_result, promised_dataframe_to_feature_group_result }

assert len(flow.edges) == 2 #these are the task dependencies

assert Edge(upstream_task=promised_get_last_executed_flow_result, downstream_task=promised_extract_from_snowflake_result, key="flow_last_executed") in flow.edges
assert Edge(upstream_task=promised_extract_from_snowflake_result, downstream_task=promised_dataframe_to_feature_group_result, key="dataframe") in flow.edges

state = flow.run()

assert state.result[promised_get_last_executed_flow_result].is_successful()
assert state.result[promised_update_last_executed_flow_result].is_successful()
assert state.result[promised_extract_from_snowflake_result].is_successful()
assert state.result[promised_dataframe_to_feature_group_result].is_successful()

state = flow.run()

assert type(state.result[promised_get_last_executed_flow_result].result) == datetime
assert state.result[promised_update_last_executed_flow_result].result == None
assert type(state.result[promised_extract_from_snowflake_result].result) == DataFrame
assert type(state.result[promised_dataframe_to_feature_group_result].result) == IngestionManagerPandas