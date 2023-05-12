import os
from flow_components import subflow_factory
from prefect import flow

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))


@flow()
def mozilla_new_tab_impressions_daily():
    """Main Prefect flow for orchestrating the ingestion of
    specific Big Query data extractions into Snowflake.  Per author
    of the original Airflow DAG, each SQL file can run in a concurrent
    workflow.
    """
    subflow_factory("mozilla_new_tab_impressions_daily")

if __name__ == "__main__":
    mozilla_new_tab_impressions_daily()
