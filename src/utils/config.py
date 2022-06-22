import os
import base64

ENV_LOCAL = "local"
ENV_DEV = "development"
ENV_PROD = "production"
PROJECT_LOCAL = "local"

ENVIRONMENT = os.getenv("ENVIRONMENT", ENV_LOCAL)
PREFECT_PROJECT_NAME = os.getenv('PREFECT_PROJECT_NAME', PROJECT_LOCAL)
PREFECT_S3_RESULT_BUCKET = os.getenv('PREFECT_S3_RESULT_BUCKET')

GCS_BUCKET = 'pocket-prefect-stage-prod' if ENVIRONMENT == ENV_PROD else 'pocket-prefect-stage-dev'
GCS_PATH_DEFAULT = ENVIRONMENT
GCS_PATH = os.getenv('GCS_PATH', GCS_PATH_DEFAULT)

SNOWFLAKE_DEV_SCHEMA_DEFAULT = 'DEV_DATA_ENGINEERING'
SNOWFLAKE_DEV_SCHEMA = os.getenv('SNOWFLAKE_DEV_SCHEMA', SNOWFLAKE_DEV_SCHEMA_DEFAULT)
SNOWFLAKE_STAGE = 'prefect.public.prefect_gcs_stage_parq_prod' if ENVIRONMENT == ENV_PROD else 'development.public.prefect_gcs_stage_parq_dev'
SNOWFLAKE_DB = 'prefect' if ENVIRONMENT == ENV_PROD else 'development'
SNOWFLAKE_MOZILLA_SCHEMA = 'mozilla' if ENVIRONMENT == ENV_PROD else SNOWFLAKE_DEV_SCHEMA
SNOWFLAKE_ANALYTICS_DATABASE = 'ANALYTICS'
SNOWFLAKE_ANALYTICS_DBT_STAGING_SCHEMA = os.getenv(
    'SNOWFLAKE_ANALYTICS_DBT_STAGING_SCHEMA',
    'DBT_STAGING'  # For local development, set the Dbt staging schema in .env
)
SNOWFLAKE_ANALYTICS_DBT_SCHEMA = os.getenv('SNOWFLAKE_ANALYTICS_DBT_SCHEMA', 'DBT')

BRAZE_API_KEY=os.getenv('BRAZE_API_KEY')
BRAZE_REST_ENDPOINT=os.getenv('BRAZE_REST_ENDPOINT')

SNOWFLAKE_DATA_RETENTION_CONNECTION_DICT = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "private_key": base64.b64decode(os.getenv("SNOWFLAKE_PRIVATE_KEY")),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "role": os.getenv("SNOWFLAKE_DATA_RETENTION_ROLE"),
    "database": os.getenv('SNOWFLAKE_DATA_RETENTION_DB'),
    "schema": os.getenv('SNOWFLAKE_DATA_RETENTION_SCHEMA')
}

SNOWPLOW_DB = os.getenv('SNOWPLOW_DB')
SNOWPLOW_SCHEMA = os.getenv('SNOWPLOW_SCHEMA')
