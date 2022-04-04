import os

ENV_LOCAL = "local"
ENV_DEV = "development"
ENV_PROD = "production"
PROJECT_LOCAL = "local"

ENVIRONMENT = os.getenv("ENVIRONMENT", ENV_LOCAL)
PREFECT_PROJECT_NAME = os.getenv('PREFECT_PROJECT_NAME', PROJECT_LOCAL)

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

BRAZE_API_KEY=os.getenv('BRAZE_API_KEY')
BRAZE_REST_ENDPOINT=os.getenv('BRAZE_REST_ENDPOINT')
