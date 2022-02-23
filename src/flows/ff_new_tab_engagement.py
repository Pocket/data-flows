from datetime import datetime, timedelta
import prefect
from prefect import Flow, task
from prefect.schedules import IntervalSchedule
from prefect.tasks.gcp.bigquery import BigQueryTask
from utils import config
from api_clients.pocket_snowflake_query import PocketSnowflakeQuery

# This Flow does the following:
#   - Export Firefox engagement data from BigQuery to GCS
#   - "submission_timestamp" column is used to pull the data
#   - UTC timestamp is used for data extracts
#   - The run schedule is expected to be 5 minutes (the table gets updated in batches ~8 mins. therefore some export
#   cycles may not return data)
#

# Setting flow variables
FLOW_NAME = "FF NewTab Engagement BQ to Snowflake Flow"
TABLE_NAME = 'impression_stats_v1'

# Export statement to export BQ data into GCS in compressed Parquet format
EXTRACT_SQL = """
        EXPORT DATA OPTIONS(
          uri=@gcs_uri,
          format='PARQUET',
          compression='SNAPPY',
          overwrite=true) AS
  
          SELECT *
          FROM `moz-fx-data-shared-prod.activity_stream_live.impression_stats_v1`
          WHERE date(submission_timestamp) >= date(@last_executed_timestamp)
          AND submission_timestamp > @last_executed_timestamp
    """

LOAD_SQL = """
        copy into {snowflake_table} (
              batch_id,
              submission_timestamp,
              additional_properties,
              addon_version,
              block,
              click,
              document_id,
              impression_id,
              loaded,
              locale,
              metadata,
              normalized_app_name,
              normalized_channel,
              normalized_country_code,
              normalized_os,
              normalized_os_version,
              page,
              pocket,
              profile_creation_date,
              region,
              release_channel,
              sample_id,
              shield_id,
              source,
              tiles,
              user_prefs,
              version,
              experiments,
              s3_filename_splitpath, 
              s3_file_row_number
            )
        from (
            select
                %(batch_id)s,
                to_timestamp($1:submission_timestamp::integer/1000000) as submission_timestamp,
                $1:additional_properties::string as additional_properties,
                $1:addon_version::string as addon_version,
                $1:block::integer as block,
                $1:click::integer as click,
                $1:document_id::string as document_id,
                $1:impression_id::string as impression_id,
                $1:loaded::integer as loaded,
                $1:locale::string as locale,
                $1:metadata as metadata,
                $1:normalized_app_name::string as normalized_app_name,
                $1:normalized_channel::string as normalized_channel,
                $1:normalized_country_code::string as normalized_country_code,
                $1:normalized_os::string as normalized_os,
                $1:normalized_os_version::string as normalized_os_version,
                $1:page::string as page,
                $1:pocket::integer as pocket,
                $1:profile_creation_date::integer as profile_creation_date,
                $1:region::string as region,
                $1:release_channel::string as release_channel,
                $1:sample_id::integer as sample_id,
                $1:shield_id::string as shield_id,
                $1:source::string as source,
                $1:tiles as tiles,
                $1:user_prefs::integer as user_prefs,
                $1:version::string as version,
                $1:experiments as experiments,
                split(metadata$filename,'/'),
                metadata$file_row_number
            from %(snowflake_stage_uri)s
        )
        file_format = (type = 'PARQUET')
        on_error=skip_file
    """


@task()
def get_last_executed_timestamp(table) -> datetime:
    # TODO: How to make this return datetime
    query = "SELECT max(submission_timestamp) FROM {table}".format(table=table)
    return PocketSnowflakeQuery()(query=query)[0][0]


@task()
def get_gcs_uri(timestamp: datetime) -> str:
    ts = timestamp.strftime('%a')
    return f'gs://{config.GCS_BUCKET}/{config.GCS_PATH}/{TABLE_NAME}/{ts}/*.parq'


@task()
def get_snowflake_stage_uri(timestamp: datetime) -> str:
    ts = timestamp.strftime('%a')
    return f'@{config.SNOWFLAKE_STAGE}/{config.GCS_PATH}/{TABLE_NAME}/{ts}/'


# Schedule to run every 5 minutes
schedule = IntervalSchedule(
    start_date=datetime.utcnow() + timedelta(seconds=1),
    interval=timedelta(minutes=5),
)

with Flow(FLOW_NAME, schedule) as flow:
    # TODO: What is best practices for constructing table names when they may vary based on environment?
    snowflake_table = f'{config.SNOWFLAKE_DB}.{config.SNOWFLAKE_MOZILLA_SCHEMA}.{TABLE_NAME}'
    last_executed_timestamp = get_last_executed_timestamp(snowflake_table)
    batch_timestamp = datetime.now()

    extract = BigQueryTask()(
        query=EXTRACT_SQL,
        query_params=[('gcs_uri', 'STRING', get_gcs_uri(batch_timestamp)),
                      ('last_executed_timestamp', 'TIMESTAMP', last_executed_timestamp)]),
    # Is it possible to return the list of files to import from extract? If so it may be interesting to pass these files
    # to the load task and load specifically those files.

    load = PocketSnowflakeQuery()(
        query=LOAD_SQL.format(snowflake_table=snowflake_table),
        data={
            'snowflake_stage_uri': get_snowflake_stage_uri(batch_timestamp),
            'batch_id': batch_timestamp.strftime('%a'),
        })

    load.set_upstream(extract)

if __name__ == "__main__":
    flow.run()
