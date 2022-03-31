import os
import boto3

from prefect import Flow, task
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from elasticsearch_dsl.query import Bool, Range, Match
from elasticsearch_dsl import Search

ES_ENDPOINT = "search-item-recs-wslncyus6txlpavliekv7bvrty.us-east-1.es.amazonaws.com"
ES_PATH = "item-rec-data_v3"
EN_US_FEED_ID = 1

def curated_by_age(feed: int, scale: str = "7d", newer: bool = True) -> Bool:
    """
    Fetch curated items newer (older) than some number of days for a specific feed. Used for getting items
    that likely are (not) live on the new tab.
    :param feed: Integer representing the curator feed to use
    :param scale: String of number of days an item must be older than
    :param newer: Bool indicating whether returned items are newer or older than `scale` days ago
    :return:
    """
    cmp_str = "gte" if newer else "lt"
    bool_query = Bool(must=[Range(approved_feeds__approved_feed_time_live={cmp_str: f"now-{scale}"})])
    bool_query.must.append(Match(approved_feeds__approved_feed_id={"query": str(feed)}))
    return bool_query


with Flow("example elasticsearch query") as flow:

    session = boto3.Session()
    credentials = session.get_credentials()
    region = os.environ.get('AWS_REGION', 'us-east-1')
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, "es",
                       session_token=credentials.token)
    es = Elasticsearch(
        hosts=[{'host': ES_ENDPOINT, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    q = curated_by_age(EN_US_FEED_ID)
    print(q.to_dict())

    response = es.search(
        index=ES_PATH,
        body=q.to_dict()
    )

    results = [x for x in response["hits"]["hits"]]
    for i in range(9):
        print(results[i]["_source"]["title"])


if __name__ == "__main__":
    flow.run()
