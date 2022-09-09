import os
from time import sleep

import boto3
import pandas as pd
import prefect
from prefect import task

from utils import config


@task()
def athena_query(query: str):
    """
    athena_query executes the query and returns the query result
    Input: query (str): query statement
    Returns: query result as Pandas DataFrame
    """

    logger = prefect.context.get("logger")
    athena = boto3.client('athena')

    # Submit Athena query for execution
    # The result sent to S3 location (config.ATHENA_S3_OUTPUT)
    response = athena.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': config.ATHENA_S3_OUTPUT,
        }
    )
    QueryExecutionId = response['QueryExecutionId']

    # Wait until successful query completion
    query_status = None
    while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
        sleep(1)
        query_execution = athena.get_query_execution(
            QueryExecutionId=QueryExecutionId
        )

        query_execution_id = query_execution['QueryExecution']['QueryExecutionId']
        query_status = query_execution['QueryExecution']['Status']['State']
        if query_status == 'FAILED':
            athena_error = query_execution['QueryExecution']['Status']['AthenaError']
            raise Exception(f'Athena query {query_execution_id} failed {athena_error}: {query}')
        elif query_status == 'CANCELLED':
            raise Exception(f'Athena query {query_execution_id} was cancelled: {query}')

    logger.info(f'Athena query {query_execution_id} completed successfully')
    df = pd.read_csv(os.path.join(config.ATHENA_S3_OUTPUT, f'{query_execution_id}.csv'))
    logger.info(f'Athena query {query_execution_id} produced {len(df)} rows')
    return df

