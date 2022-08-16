whileimport boto3
import pandas as pd
from time import sleep
from prefect import task
from utils import config

@task()
def athena_query(query: str):
    """
    athena_query executes the query and returns the query result
    Input: query (str): query statement
    Returns: query result as Pandas DataFrame
    """

    client = boto3.client('athena')

    # Submit Athena query for execution
    # The result sent to S3 location (config.ATHENA_S3_OUTPUT)
    response = client.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': config.ATHENA_S3_OUTPUT,
        }
    )
    QueryExecutionId = response['QueryExecutionId']

    # Wait until successful query completion
    query_status = None
    while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
        query_status = client.get_query_execution(
            QueryExecutionId=QueryExecutionId
        )['QueryExecution']['Status']['State']
        if query_status == 'FAILED' or query_status == 'CANCELLED':
            raise Exception('Athena query "{}" failed or was cancelled'.format(query))
        sleep(1)

    # Can you see this?
    next_token_param = {}
    has_next_token = True
    rows = []
    columns = []
    while has_next_token is not None:

        # Get query results
        response = client.get_query_results(
            QueryExecutionId=QueryExecutionId,
            MaxResults=1000,
            **next_token_param
        )
        if not columns:
            columns = [col.get('VarCharValue') for col in response['ResultSet']['Rows'][0]['Data']]
        rows += [[data.get('VarCharValue') for data in row['Data']] for row in response['ResultSet']['Rows'][1:]]

        next_token = response.get('NextToken')
        has_next_token = next_token is not None
        next_token_param = {'NextToken': next_token}

    df = pd.DataFrame(rows, columns=columns)
    return df
