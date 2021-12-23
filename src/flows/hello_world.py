from os import environ

import prefect
from prefect import task, Flow
from prefect.run_configs import ECSRun
import pandas
import pyarrow
import fastparquet


@task
def log_versions():
    logger = prefect.context.get("logger")
    logger.info(f"pandas version = {pandas.__version__}")
    logger.info(f"pyarrow version = {pyarrow.__version__}")
    logger.info(f"fastparquet version = {fastparquet.__version__}")


with Flow("hello_world") as flow:
    log_versions()

flow.run()

# TODO: In production, the steps below would be taken by a deployment script. They're just included here as an example.
# flow.storage = prefect.storage.S3(
#     bucket=f"pocket-dataflows-storage-{environ.get('ENVIRONMENT').lower()}",
#     add_default_labels=False,
# )
#
# flow.run_config = ECSRun(
#     labels=[environ.get('ENVIRONMENT')],
#     # Set to the same image as the DataFlows-Prod or DataFlows-Dev task definition.
#     image=environ.get('PREFECT_IMAGE'),
# )
#
# flow.register(project_name="prefect-tutorial")
