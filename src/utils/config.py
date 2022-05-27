import os

ENV_LOCAL = "local"
ENV_DEV = "development"
ENV_PROD = "production"
PROJECT_LOCAL = "local"

SRC_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))

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
SNOWFLAKE_DATA_RETENTION_USER = ""
SNOWFLAKE_DATA_RETENTION_PRIVATE_KEY = ""
SNOWFLAKE_DATA_RETENTION_ROLE = ""
SNOWFLAKE_DATA_RETENTION_WAREHOUSE = ""

BRAZE_API_KEY = os.getenv('BRAZE_API_KEY')
BRAZE_REST_ENDPOINT = os.getenv('BRAZE_REST_ENDPOINT')


SNOWFLAKE_DATA_RETENTION_CONNECTION_DICT = {
    "user": os.environ.get("SNOWFLAKE_DATA_RETENTION_USER"),
    "private_key": os.environ.get("SNOWFLAKE_DATA_RETENTION_PRIVATE_KEY"),
    "role": os.environ.get("SNOWFLAKE_DATA_RETENTION_ROLE"),
    "warehouse": os.environ.get("SNOWFLAKE_DATA_RETENTION_WAREHOUSE"),
    "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
}

