from prefect import Flow

from api_clients.pocket_snowflake_query import PocketSnowflakeQuery

with Flow("example Snowflake query") as flow:
    s3download_result = PocketSnowflakeQuery()(
        query="SELECT 'row-1' as pk,2,3 HAVING pk != %s",
        data=('row-2',)
    )

if __name__ == "__main__":
    flow.run()
