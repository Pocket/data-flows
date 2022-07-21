import logging
import sys
import urllib
import json
import time
import uuid
import boto3
import os
import pandas as pd
from prefect import task

from collections import Counter
from typing import Tuple, Dict, List, Union, Set
from typing import Optional, List
from pydantic import BaseModel, root_validator
from enum import Enum

from utils.config import SQS_MESSAGE_VERSION, SQS_REC_QUEUE, SQS_PROSPECT_QUEUE

ONE_DAY_S = 86400
ONE_WEEK_S = ONE_DAY_S * 7


# todo: remove sqs when we finished migration to SageMaker Feature Store

@dataclass
class SQSInfo:
    name: CandidateType
    sqs_queue: str


class SQSConfig:
    recommendation = SQSInfo(name=CandidateType.recommendation,
                             sqs_queue=SQS_REC_QUEUE)
    prospecting = SQSInfo(name=CandidateType.prospect,
                          sqs_queue=SQS_PROSPECT_QUEUE)


class RecommendationCandidate(BaseModel):
    item_id: int
    publisher: str
    feed_id: Optional[int] = None


class CurationProspect(BaseModel):
    scheduled_surface_guid: str
    prospect_id: str
    url: str
    prospect_source: str
    save_count: int
    predicted_topic: str
    rank: int


class CandidateType(str, Enum):
    prospect = "prospect"
    recommendation = "recommendation"


class CandidateSet(BaseModel):
    id: str
    version: int
    candidates: Union[List[RecommendationCandidate], List[CurationProspect]]
    type: CandidateType
    flow: str
    run: str
    expires_at: int

    @root_validator(pre=False)
    def validate_list_elements(cls, values):
        expected_type = CurationProspect if values.get("type") == CandidateType.prospect else RecommendationCandidate
        for c in values.get("candidates"):
            assert type(c) == expected_type, f"expected {expected_type} : got {type(c)}"
        return values


class NewTabFeedID(int, Enum):
    en_US = 1
    de_DE = 3
    en_GB = 6
    en_INTL = 8


@task()
def put_results(candidate_set_id: str,
                candidates: Union[List[RecommendationCandidate], List[CurationProspect]],
                expires_in_s: int = ONE_WEEK_S * 4,
                sqs_queue_name: str = SQSConfig.recommendation.sqs_queue,
                curated: bool = False,
                candidate_type: CandidateType = CandidateType.recommendation):
    # Check that the candidate_set_id is actually a GUID
    uuid.UUID(candidate_set_id)

    # curated items are required to have a feed_id, these are generally optional
    if curated:
        for c in candidates:
            assert type(c.feed_id) == int, f"{c.item_id} has feed_id={c.feed_id} in a curated flow"

    # note there are extra fields in this model not present in the recommendation-api
    # candidate set model, however they should pass through
    candidate_set = CandidateSet(id=candidate_set_id,
                                 flow=current.flow_name,
                                 run=current.run_id,
                                 expires_at=int(time.time() + expires_in_s),
                                 type=candidate_type,
                                 version=SQS_MESSAGE_VERSION,
                                 candidates=candidates)

    logger = prefect.context.get("logger")
    message = json.dumps(candidate_set.dict())
    logger.info(f'SQS message: {message}')

    if expires_in_s <= 0:
        raise AssertionError(f"Invalid expiry set: {expires_in_s=}")

    message = json.dumps(candidate_set.dict())
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=sqs_queue_name)
    logger.info(f'Sent {len(candidates)} candidates from {candidate_set_id}')
    sqs_result = queue.send_message(MessageBody=message)
    logger.info(f'SQS result: {sqs_result}')
