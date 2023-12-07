import json
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Optional, List
from typing import Union, Dict

import boto3
import prefect
from prefect import task, get_run_logger
from prefect.context import FlowRunContext
from pydantic import BaseModel, root_validator
from common.settings import CommonSettings

CS = CommonSettings()  # type: ignore

SQS_MESSAGE_VERSION = 3
SQS_REC_QUEUE = (
    "RecommendationAPI-Prod-Sqs-Translation-Queue"
    if CS.is_production
    else "RecommendationAPI-Dev-Sqs-Translation-Queue"
)
SQS_PROSPECT_QUEUE = (
    "ProspectAPI-Prod-Sqs-Translation-Queue"
    if CS.is_production
    else "ProspectAPI-Dev-Sqs-Translation-Queue"
)

ONE_DAY_S = 86400
ONE_WEEK_S = ONE_DAY_S * 7


# todo: remove sqs when we finished migration to SageMaker Feature Store


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
        expected_type = (
            CurationProspect
            if values.get("type") == CandidateType.prospect
            else RecommendationCandidate
        )
        for c in values.get("candidates"):
            assert type(c) == expected_type, f"expected {expected_type} : got {type(c)}"
        return values


class NewTabFeedID(int, Enum):
    en_US = 1
    de_DE = 3
    en_GB = 6
    en_INTL = 8


@dataclass
class SQSInfo:
    name: CandidateType
    sqs_queue: str


class SQSConfig:
    recommendation = SQSInfo(name=CandidateType.recommendation, sqs_queue=SQS_REC_QUEUE)
    prospecting = SQSInfo(name=CandidateType.prospect, sqs_queue=SQS_PROSPECT_QUEUE)


@task()
# Move this out of SQS class to a more appropriate place -- it's a data validator - probably legacy_flow.py
def validate_candidate_items(corpus_items: List[Dict]):
    min_item_count = 1
    expected_keys = ["ID", "PUBLISHER"]

    assert len(corpus_items) >= min_item_count
    assert all(
        set(expected_keys).difference(set(corpus_item.keys())) == set()
        for corpus_item in corpus_items
    )
    assert all(
        isinstance(corpus_item["ID"], int) and corpus_item["ID"] != None
        for corpus_item in corpus_items
    )
    assert all(
        isinstance(corpus_item["PUBLISHER"], str) and corpus_item["PUBLISHER"] != ""
        for corpus_item in corpus_items
    )

    return corpus_items


@task()
def put_results(
    candidate_set_id: str,
    candidates: Union[List[RecommendationCandidate], List[CurationProspect]],
    expires_in_s: int = ONE_WEEK_S * 4,
    sqs_queue_name: str = SQSConfig.recommendation.sqs_queue,
    curated: bool = False,
    candidate_type: CandidateType = CandidateType.recommendation,
):
    # Check that the candidate_set_id is actually a GUID
    uuid.UUID(candidate_set_id)

    # curated items are required to have a feed_id, these are generally optional
    if curated:
        for c in candidates:
            assert (
                type(c.feed_id) == int
            ), f"{c.item_id} has feed_id={c.feed_id} in a curated flow"

    flow_run = FlowRunContext.get().flow_run.dict()

    # note there are extra fields in this model not present in the recommendation-api
    # candidate set model, however they should pass through
    candidate_set = CandidateSet(
        id=candidate_set_id,
        flow=flow_run["name"],
        run=str(flow_run["id"]),
        expires_at=int(time.time() + expires_in_s),
        type=candidate_type,
        version=SQS_MESSAGE_VERSION,
        candidates=candidates,
    )

    logger = get_run_logger()
    message = json.dumps(candidate_set.dict())
    logger.info(f"SQS message: {message}")

    if expires_in_s <= 0:
        raise AssertionError(f"Invalid expiry set: {expires_in_s=}")

    message = json.dumps(candidate_set.dict())
    boto3_session = boto3.session.Session()  # Each thread needs its own boto3 session
    sqs = boto3_session.resource("sqs")
    queue = sqs.get_queue_by_name(QueueName=sqs_queue_name)
    logger.info(f"Sent {len(candidates)} candidates from {candidate_set_id}")
    sqs_result = queue.send_message(MessageBody=message)
    logger.info(f"SQS result: {sqs_result}")
