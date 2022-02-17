import os

ENV_LOCAL = "local"
ENV_DEV = "development"
ENV_PROD = "production"
PROJECT_LOCAL = "local"
GCS_PATH_DEFAULT = "/"

ENVIRONMENT = os.getenv("ENVIRONMENT", ENV_LOCAL)
PREFECT_PROJECT_NAME = os.getenv('PREFECT_PROJECT_NAME', PROJECT_LOCAL)

GCS_BUCKET = 'pocket-prefect-stage-prod' if ENVIRONMENT == ENV_PROD else 'pocket-prefect-stage-dev'
GCS_PATH = os.getenv('GCS_PATH', GCS_PATH_DEFAULT)
