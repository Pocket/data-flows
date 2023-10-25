# import base64
# import os.path
# import tempfile
# import zlib
# from io import BytesIO

# import boto3
# import pandas as pd
# import prefect
# from prefect import Flow, task, Parameter
# from prefect.executors import LocalDaskExecutor
# from prefect.tasks.aws import S3List

# from utils.flow import get_flow_name

# # Setting flow variables
# FLOW_NAME = get_flow_name(__file__)

# # This bucket was created by another process. We may have to revisit using this bucket.
# SOURCE_S3_BUCKET = 'pocket-snowflake-staging-manual'
# SOURCE_PREFIX = 'aurora/textparser-prod-content-snapshot-2022091408-cluster/content/'
# STAGE_S3_BUCKET = 'pocket-data-items'
# STAGE_S3_PREFIX = 'article/backfill-html-filesplit'
# STAGE_CHUNK_ROWS = 50000
# NUM_FILES_PER_RUN = 1000


# @task()
# def get_source_keys(source_prefix: str, num_files: int) -> [str]:
#     keys = S3List().run(bucket=SOURCE_S3_BUCKET, prefix=source_prefix, max_items=num_files)
#     keys.reverse()
#     return keys


# def transform(df: pd.DataFrame) -> pd.DataFrame:
#     df['html'] = [zlib.decompress(base64.b64decode(compressed_html)).decode() for compressed_html in df['compressed_html']]
#     df.drop(columns=['compressed_html'], inplace=True)
#     return df


# def split_file(key: str):
#     bucket = SOURCE_S3_BUCKET
#     logger = prefect.context.get("logger")
#     logger.info(f"Extracting file: {str(key)}")
#     s3 = boto3.client('s3')
#     with tempfile.TemporaryFile() as f:
#         s3.download_fileobj(bucket, key, f)
#         f.seek(0)
#         for chunk in pd.read_csv(f, escapechar='\\', sep="|", header=None, usecols=[0, 1],
#                                  chunksize=STAGE_CHUNK_ROWS,
#                                  names=['resolved_id', 'compressed_html']):
#             yield chunk


# def stage_chunk(key: str, index: int, df: pd.DataFrame) -> str:
#     logger = prefect.context.get("logger")
#     bucket = STAGE_S3_BUCKET
#     s3_prefix = STAGE_S3_PREFIX
#     s3 = boto3.resource('s3')

#     file_name = os.path.basename(key)
#     stage_key = f"{s3_prefix}/{file_name}_{index}.pickle.gz"

#     logger.info(f"stage_key: {stage_key}.")
#     buffer = BytesIO()
#     df.to_pickle(buffer, compression='gzip')
#     s3.Object(bucket, stage_key).put(Body=buffer.getvalue())

#     return stage_key


# @task()
# def split_files_process(key: str):
#     """
#     Split S3 files into smaller stage files
#     """
#     index = -1
#     for df in split_file(key):
#         index += 1
#         stage_chunk(key=key, index=index, df=transform(df))


# with Flow(FLOW_NAME, executor=LocalDaskExecutor(scheduler="threads")) as flow:
#     num_files = Parameter('num_files', default=NUM_FILES_PER_RUN)
#     source_prefix = Parameter('source_prefix', default=SOURCE_PREFIX)
#     keys = get_source_keys(source_prefix, num_files)
#     split_files_process.map(keys)

# if __name__ == "__main__":
#     flow.run()
