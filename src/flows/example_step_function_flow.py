from os import environ
import json

import prefect
from prefect import task, Flow
from prefect.run_configs import ECSRun
from prefect.tasks.aws.step_function import StepActivate


activate_step_function = StepActivate(
    state_machine_arn='arn:aws:states:us-east-1:410318598490:stateMachine:TimespentProspectsFlow',
    execution_name='12345676-3958-5018-747d-9be223a42df1_73589d9c-bb9e-0d70-2fd5-557821bd475e',
    execution_input=json.dumps({
      "Parameters": "{}",
    }),
)


with Flow("step_function_flow") as flow:
    step_function_resut = activate_step_function()

# flow.run()

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
