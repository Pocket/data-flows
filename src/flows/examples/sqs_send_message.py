import json
import uuid
import time
from typing import Dict

import boto3
from prefect import Flow, context, task

from utils.flow import get_flow_name
from utils import config

FLOW_NAME = get_flow_name(__file__)

ENVIRONMENT_SHORT_NAME = 'Prod' if config.ENVIRONMENT == config.ENV_PROD else 'Dev'
SQS_QUEUE_NAME = f'RecommendationAPI-{ENVIRONMENT_SHORT_NAME}-Sqs-Translation-Queue'


@task()
def create_example_candidate_set():
    return {
        "id": "71e296d5-1732-448e-ac52-a2a240fec12b",  # Example candidate set
        "version": 3,
        "candidates": [
            {"item_id": 3565965636, "publisher": "getpocket.com", "feed_id": None},
            {"item_id": 3445017664, "publisher": "getpocket.com", "feed_id": None},
            {"item_id": 3455519696, "publisher": "getpocket.com", "feed_id": None},
            {"item_id": 3520707683, "publisher": "getpocket.com", "feed_id": None},
            {"item_id": 3433651158, "publisher": "getpocket.com", "feed_id": None},
            {"item_id": 3522243744, "publisher": "getpocket.com", "feed_id": None}],
        "type": "recommendation",
        "flow": "SyndicatedFlow",
        "run": "63347",
        "expires_at": 1657743903
    }


@task()
def send_sqs_message(obj: Dict):
    boto_session = boto3.Session()
    sqs = boto_session.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=SQS_QUEUE_NAME)

    message = json.dumps(obj)
    logger = context.get("logger")
    logger.info(f"Sending message to {SQS_QUEUE_NAME}: {obj}")
    sqs_result = queue.send_message(MessageBody=message)
    logger.info({sqs_result})


with Flow(FLOW_NAME) as flow:
    candidate_set = create_example_candidate_set()
    send_sqs_message(candidate_set)

if __name__ == "__main__":
    flow.run()
