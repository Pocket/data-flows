import os

ENV_LOCAL = "local"
ENV_DEV = "development"
ENV_PROD = "production"
PROJECT_LOCAL = "local"
GCS_PATH_DEFAULT = "/"
SNOWFLAKE_DEV_SCHEMA_DEFAULT = 'XXX_DATA_ENGINEERING'

ENVIRONMENT = os.getenv("ENVIRONMENT", ENV_LOCAL)
PREFECT_PROJECT_NAME = os.getenv('PREFECT_PROJECT_NAME', PROJECT_LOCAL)
SNOWFLAKE_DEV_SCHEMA = os.getenv('SNOWFLAKE_DEV_SCHEMA', SNOWFLAKE_DEV_SCHEMA_DEFAULT)

GCS_BUCKET = 'pocket-prefect-stage-prod' if ENVIRONMENT == ENV_PROD else 'pocket-prefect-stage-dev'
SNOWFLAKE_STAGE = 'prefect.public.prefect_gcs_stage_parq_prod' if ENVIRONMENT == ENV_PROD else 'development.public.prefect_gcs_stage_parq_dev'
GCS_PATH = os.getenv('GCS_PATH', GCS_PATH_DEFAULT)
SNOWFLAKE_DB = 'prefect' if ENVIRONMENT == ENV_PROD else 'development'
SNOWFLAKE_MOZILLA_SCHEMA = 'mozilla' if ENVIRONMENT == ENV_PROD else SNOWFLAKE_DEV_SCHEMA
