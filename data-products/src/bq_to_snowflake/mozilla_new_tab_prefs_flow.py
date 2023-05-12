from flow_components import subflow_factory
from prefect import flow


@flow()
def mozilla_new_tab_prefs():
    """Main Prefect flow for orchestrating the ingestion of
    specific Big Query data extractions into Snowflake.  Per author
    of the original Airflow DAG, each SQL file can run in a concurrent
    workflow.
    """
    subflow_factory("mozilla_new_tab_prefs")


if __name__ == "__main__":
    mozilla_new_tab_prefs()
