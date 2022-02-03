import os

ENV_LOCAL = "local"
ENV_DEV = "development"
ENV_PROD = "production"
PROJECT_LOCAL = "local"

ENVIRONMENT = os.getenv("ENVIRONMENT", ENV_LOCAL)
PREFECT_PROJECT_NAME = os.getenv('PREFECT_PROJECT_NAME', PROJECT_LOCAL)
