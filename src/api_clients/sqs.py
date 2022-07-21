import logging
import sys
import urllib
import json
import time
import uuid
import boto3
import os
import pandas as pd

from collections import Counter
from typing import Tuple, Dict, List, Union, Set
from metaflow import current, Run

from utils.config import SQSConfig, FIREHOSE_NAME, SQS_MESSAGE_VERSION, ENVIRONMENT, ENV_PROD
from api_clients.models import RecommendationCandidate, CurationProspect, CandidateType, CandidateSet

ONE_DAY_S = 86400
ONE_WEEK_S = ONE_DAY_S * 7


class NewTabFeedID(int, Enum):
    en_US = 1
    de_DE = 3
    en_GB = 6
    en_INTL = 8


def put_results(candidate_set_id: str,
                candidates: Union[List[RecommendationCandidate], List[CurationProspect]],
                expires_in_s: int = ONE_WEEK_S * 4,
                sqs_queue_name: str = SQSConfig.recommendation.sqs_queue,
                firehose_name: str = FIREHOSE_NAME,
                curated: bool = False,
                candidate_type: CandidateType = CandidateType.recommendation,
                sqs_verbose: bool = False):
    """Puts a candidate set onto an SQS queue. Will check to make sure this is a production run.
    :param candidate_set_id: GUID representing this candidate set
    :param candidates: List of candidates for this candidate set
    :param expires_in_s: When this candidate set should expire in seconds from now().
    :param sqs_queue_name: Name of the queue
    :param firehose_name: Name of the kinesis stream to emit candidate sets to
    :param curated: flag indicating a curated candidate set
    :param candidate_type: CandidateType differentiates recommendation candidate sets and curation prospect sets
    :param sqs_verbose: flag to print sqs message
    """

    # Check that the candidiate_set_id is actually a GUID
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

    if sqs_verbose:
        message = json.dumps(candidate_set.dict())
        print("====== SQS MESSAGE")
        print(message)
        print("====== SQS MESSAGE")

    if expires_in_s <= 0:
        raise AssertionError(f"Invalid expiry set: {expires_in_s=}")

    if ENVIRONMENT == ENV_PROD:
        message = json.dumps(candidate_set.dict())
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=sqs_queue_name)
        logging.info("Sent %d candidates from %s", len(candidates), candidate_set_id)
        sqs_result = queue.send_message(MessageBody=message)

        print(f"posting candidate set from {current.flow_name} with type {candidate_type} to firehose")
        firehose = boto3.client('firehose')
        # Firehose may batch records together, so they want newline delimited records
        # See https://docs.aws.amazon.com/firehose/latest/APIReference/API_PutRecord.html
        # So we are delivering new-line delimited UTF-8 encoded JSON
        firehose_message = message + '\n'
        firehose_result = firehose.put_record(DeliveryStreamName=firehose_name,
                                              Record={"Data": firehose_message.encode('utf-8')})
        return sqs_result, firehose_result
    else:
        print(f"put_results called from non-production {candidate_type} job; skipping put_results_sqs")


# A Result set is a tuple, consisting of a guid, candidate list (recommended or curated prospects),
# and a boolean indicating whether the candidate set is curated or not.
ResultSetType = Tuple[str, Union[List[RecommendationCandidate], List[CurationProspect]], bool]
ResultSetBatchType = List[ResultSetType]


def put_results_batch(results: ResultSetBatchType) -> Dict:
    """Puts a bunch of results at once, returning a dictionary where keys are GUIDs are values are the responses"""
    responses = {}

    # Ensure GUIDS are unique
    guids = Counter([guid for guid, _, _ in results])
    guid_dupes = [guid for guid, cnt in guids.items() if cnt > 1]
    if guid_dupes:
        raise ValueError(f"Duplicate GUIDs detected: {guid_dupes}")

    for guid, cset, is_curated in results:
        responses[guid] = put_results(guid, cset, curated=is_curated)

    return responses
