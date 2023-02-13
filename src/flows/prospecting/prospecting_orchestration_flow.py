import json

import boto3
import boto3.session
from pendulum import now as pd_now
from prefect import Flow, task, unmapped
from prefect.executors import DaskExecutor
from prefect.tasks.aws import StepActivate

from utils.config import (
    ENVIRONMENT,
    PROSPECTING_PROSPECTS_FLOWS,
    PROSPECTING_TRAINING_FLOWS,
)
from utils.flow import get_flow_name, get_interval_schedule

FLOW_NAME = get_flow_name(__file__)

# List of step function workflows to execute.
STEP_FUNCTION_TRAINING_FLOWS = PROSPECTING_TRAINING_FLOWS[ENVIRONMENT]
STEP_FUNCTION_PROSPECTS_FLOWS = PROSPECTING_PROSPECTS_FLOWS[ENVIRONMENT]

# Base input to pass to the step function execution
BASE_EXECUTION_INPUT = json.dumps(
    {
        "Parameters": "{}",
    }
)


@task()
def create_sfn_arns(sfn_flow_names: list) -> list:
    # Create your session as suggested for threading
    init_session = boto3.session.Session()
    # get region name and account id for return list
    region_name = init_session.region_name
    sts = init_session.client("sts")
    account_id = sts.get_caller_identity()["Account"]
    return [
        f"arn:aws:states:{region_name}:{account_id}:stateMachine:{x}"
        for x in sfn_flow_names
    ]


@task()
def create_sfn_execution_names(sfn_flow_names: list) -> list:
    return [f"{x}-{pd_now('UTC').int_timestamp}" for x in sfn_flow_names]


with Flow(
    FLOW_NAME, schedule=get_interval_schedule(minutes=120), executor=DaskExecutor()
) as flow:
    # create the list of sfn flow arns and execution names
    training_arns = create_sfn_arns(STEP_FUNCTION_TRAINING_FLOWS)
    prospects_arns = create_sfn_arns(STEP_FUNCTION_PROSPECTS_FLOWS)
    training_exectuion_names = create_sfn_execution_names(STEP_FUNCTION_TRAINING_FLOWS)
    prospects_execution_names = create_sfn_execution_names(
        STEP_FUNCTION_PROSPECTS_FLOWS
    )

    # execute step function flows from list of tuples(name, arn)
    training_flows = StepActivate(name="training_flow").map(
        state_machine_arn=training_arns,
        execution_name=training_exectuion_names,
        execution_input=unmapped(BASE_EXECUTION_INPUT),
    )
    StepActivate(name="prospects_flow").map(
        state_machine_arn=prospects_arns,
        execution_name=prospects_execution_names,
        execution_input=unmapped(BASE_EXECUTION_INPUT),
    ).set_upstream(training_flows)

# for execution in development only
if __name__ == "__main__":
    # flow.visualize()
    flow.run()
