from datetime import datetime

from prefect import Flow, task
from prefect.tasks.aws import StepActivate

PROSPECTING_STEP_FUNCTIONS = [
    'TimespentProspectModelTrainingFlow'
]

@task
def execute_step(name, timestamp):
    arn = 'arn:aws:states:us-east-1:996905175585:stateMachine:{}'.format(name)
    execution_name = 'Prefect Prospect {fn} {timestamp}'.format(fn=name, timestamp=timestamp)
    StepActivate(state_machine_arn=arn, execution_name=execution_name)

with Flow('Prospect') as flow:
    timestamp = datetime.now().strftime("%Y%m%d %H%M%S")
    execute_step.map(PROSPECTING_STEP_FUNCTIONS, timestamp)
