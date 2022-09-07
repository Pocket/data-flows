import asyncio
import base64
import datetime
import gzip
from typing import Optional, List, Sequence, Dict

import aioboto3
import pyarrow.parquet as pq
import msgpack
import s3fs
import prefect
from prefect import Flow, Parameter, task
from prefect.executors import LocalDaskExecutor

from api_clients.models import load_dir
from utils.config import ENVIRONMENT
from utils.flow import get_flow_name
from utils.iteration import chunks

FLOW_NAME = get_flow_name(__file__)

s3_bucket_source = 'pocket-data-learning-dev'
s3_bucket_key='analytics-modeled-data/parquet/dbt/web_explore_impressions_clicks_by_item_hour/data_0_0_0.snappy.parquet'

doc2vec = load_dir('/model')


@task()
def list_files(uri):
    s3 = s3fs.S3FileSystem(anon=False)
    paths = s3.ls(uri)
    return paths


async def download_article_text(s3_client, resolved_id: int) -> Optional[str]:
    try:
        obj = await s3_client.get_object(Bucket='pocket-data-items', Key=f'article/text/{resolved_id}.gz')
    except s3_client.exceptions.NoSuchKey as e:
        return None

    body = await obj['Body'].read()
    text = gzip.decompress(body).decode('utf-8')
    return text


def create_feature_record(
        resolved_id: int,
        document_vector: List[float],
        updated_at: datetime.datetime = datetime.datetime.now(),
) -> Sequence[Dict]:
    return [
        {'FeatureName': 'resolved_id', 'ValueAsString': str(resolved_id)},
        {'FeatureName': 'updated_at', 'ValueAsString': updated_at.strftime("%Y-%m-%dT%H:%M:%SZ")},
        {'FeatureName': 'document_vector',
         'ValueAsString': base64.b64encode(msgpack.packb(document_vector, use_bin_type=False)).decode()},
    ]


def get_resolved_ids(s3_uri) -> List[int]:
    s3 = s3fs.S3FileSystem(anon=False)
    dataset = pq.ParquetDataset(
        s3_uri,
        filesystem=s3)
    return dataset.read()['RESOLVED_ID'].to_pylist()


async def process_resolved_id(resolved_id: int, s3_client, featurestore, logger):
    article_text = await download_article_text(s3_client, resolved_id)
    if not article_text:
        logger.warning(f'No article text found for resolved_id {resolved_id}')
        return

    try:
        document_vector = doc2vec.infer(article_text)
    except ValueError as e:
        logger.info(e)
        return

    record = create_feature_record(resolved_id=resolved_id, document_vector=document_vector)
    await featurestore.put_record(FeatureGroupName=f"{ENVIRONMENT}-resolved-item-vectors-v1", Record=record)

    logger.info(f"Created and stored vector for {resolved_id}")


async def async_process_resolved_ids(resolved_ids: Sequence[int], logger):
    boto_session = aioboto3.Session()
    async with boto_session.client('s3') as s3_client:
        async with boto_session.client('sagemaker-featurestore-runtime') as featurestore:
            await asyncio.gather(*(process_resolved_id(r, s3_client, featurestore, logger) for r in resolved_ids))


@task()
def process_file(s3_uri):
    logger = prefect.context.get("logger")

    resolved_id_chunks = list(chunks(get_resolved_ids(s3_uri), 100))
    for index, chunk in enumerate(resolved_id_chunks):
        logger.info(f'Starting chunk {index}/{len(resolved_id_chunks)}')
        asyncio.run(async_process_resolved_ids(chunk, logger=logger))


with Flow(FLOW_NAME, executor=LocalDaskExecutor(scheduler="processes", num_workers=8)) as flow:
    suggested_tag_s3_uri = Parameter(
        'suggested_tag_s3_uri',
        default='s3://pocket-data-learning/analytics-modeled-data/parquet/suggested_tags/tagged_items')

    s3_uris = list_files(suggested_tag_s3_uri)

    process_file.map(s3_uris)

if __name__ == "__main__":
    flow.run()
