from os import environ

import prefect
from prefect import task, Flow
from prefect.run_configs import ECSRun


@task
def abc(data):
    logger = prefect.context.get("logger")
    logger.info(f"{data['result']}")
    return {'result': 'I said Hello'}


with Flow("ecs_test") as flow:
    result = abc({'result': 'hello world'})
    abc(result)

# TODO: In production, the steps below would be taken by a deployment script. They're just included here as an example.
flow.storage = prefect.storage.S3(
    bucket='pocket-dataflows-storage-dev',
    add_default_labels=False
)

flow.run_config = ECSRun(
    # task_definition_path="test.yaml",
    labels=['Dev'],
    task_role_arn=environ.get('PREFECT_TASK_ROLE_ARN'),
    # execution_role_arn='arn:aws:iam::12345678:role/prefect-ecs',
    image='prefecthq/prefect:latest-python3.9',
)

flow.register(project_name="prefect-tutorial")
