import os

ENV_LOCAL = "local"
ENV_DEV = "development"
ENV_PROD = "production"
PROJECT_LOCAL = "local"
GCS_PATH_DEFAULT = "/"

ENVIRONMENT = os.getenv("ENVIRONMENT", ENV_LOCAL)
PREFECT_PROJECT_NAME = os.getenv('PREFECT_PROJECT_NAME', PROJECT_LOCAL)

GCS_BUCKET = 'pocket-airflow-prod-stage' if ENVIRONMENT == ENV_PROD else 'pocket-airflow-nonprod-stage'
GCS_PATH = os.getenv('GCS_PATH', GCS_PATH_DEFAULT)
