import os

ENV_LOCAL = "local"
ENV_DEV = "development"
ENV_PROD = "production"

ENV = os.getenv("ENVIRONMENT", ENV_LOCAL)
