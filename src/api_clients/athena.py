import boto3
import pandas as pd
from time import sleep
from prefect import task
from utils import config

@task()
def AthenaQuery(query: str = None):

    client = boto3.client('athena')

    response = client.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': config.ATHENA_S3_OUTPUT,
        }
    )
    QueryExecutionId = response['QueryExecutionId']

    query_status = None
    while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
        query_status = client.get_query_execution(
            QueryExecutionId=QueryExecutionId
        )['QueryExecution']['Status']['State']
        if query_status == 'FAILED' or query_status == 'CANCELLED':
            raise Exception('Athena query "{}" failed or was cancelled'.format(query))
        sleep(1)

    response = client.get_table_metadata(
        CatalogName="AwsDataCatalog",
        DatabaseName="sagemaker_featurestore",
        TableName="development-user-recommendation-preferences-v1-1654826050",
    )
    columns = [
        col['Name']
        for col in response['TableMetadata']['Columns']
    ]

    response = client.get_query_results(
        QueryExecutionId=QueryExecutionId,
        MaxResults=20
    )
    results = [[data.get('VarCharValue') for data in row['Data']] for row in response['ResultSet']['Rows'][1:]]

    df = pd.DataFrame(results, columns=columns)
    return df
