import json
from prefect import Flow
import uuid

from prefect.tasks.aws import StepActivate

from utils.flow import get_flow_name

FLOW_NAME = get_flow_name(__file__)


with Flow(FLOW_NAME) as flow:
    step_function_result = StepActivate()(
        execution_name=str(uuid.uuid4()),
        state_machine_arn='arn:aws:states:us-east-1:410318598490:stateMachine:TimespentProspectsFlow',
        execution_input=json.dumps({
            "Parameters": "{}",
        }),
    )

if __name__ == "__main__":
    flow.run()
