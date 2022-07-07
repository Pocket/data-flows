import datetime
import json
from typing import Dict, Sequence

import boto3
from prefect import task, Parameter
from sagemaker import Session
from sagemaker.feature_store.feature_group import FeatureGroup
from sagemaker.feature_store.inputs import FeatureValue

from utils import config


@task()
def create_corpus_candidate_set_record(
        id: str,
        corpus_items: Dict,
        unloaded_at: datetime.datetime = datetime.datetime.now()
) -> Sequence[FeatureValue]:
    return [
        FeatureValue('id', id),
        FeatureValue('unloaded_at', unloaded_at.strftime("%Y-%m-%dT%H:%M:%SZ")),
        FeatureValue('corpus_items', json.dumps(corpus_items)),
    ]


@task()
def load_feature_record(record: Sequence[FeatureValue], feature_group_name):
    boto_session = boto3.Session()
    feature_store_session = Session(boto_session=boto_session,
                                    sagemaker_client=boto_session.client(service_name='sagemaker'),
                                    sagemaker_featurestore_runtime_client=boto_session.client(service_name='sagemaker-featurestore-runtime'))
    feature_group = FeatureGroup(name=feature_group_name, sagemaker_session=feature_store_session)
    feature_group.put_record(record)


feature_group = Parameter("feature group", default=f"{config.ENVIRONMENT}-corpus-candidate-sets-v1")
