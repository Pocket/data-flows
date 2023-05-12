from flow_components import subflow_factory
from prefect import flow
import asyncio


@flow()
async def mozilla_new_tab_impressions_daily():
    """Main Prefect flow for orchestrating the ingestion of
    specific Big Query data extractions into Snowflake.  Per author
    of the original Airflow DAG, each SQL file can run in a concurrent
    workflow.
    """
    await subflow_factory("mozilla_new_tab_impressions_daily")


if __name__ == "__main__":
    asyncio.run(mozilla_new_tab_impressions_daily())
