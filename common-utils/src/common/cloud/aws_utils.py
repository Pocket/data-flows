import json
import logging
import os
from typing import Any

from boto3 import Session
from botocore.exceptions import ClientError
from pydantic import BaseSettings
from slugify import slugify

# Create logger for module
LOGGER_NAME = __name__
LOGGER = logging.getLogger(LOGGER_NAME)


def fetch_aws_secret(settings: BaseSettings) -> dict[str, Any]:
    sm = Session().client(
        service_name="secretsmanager",
    )

    def _get_secret(secret_name: str) -> None | str | dict[str, Any]:
        secret_string = None
        try:
            secret_string = sm.get_secret_value(SecretId=secret_name)["SecretString"]
            return json.loads(secret_string)
        except json.decoder.JSONDecodeError:
            return secret_string
        except ClientError as e:
            LOGGER.debug(e)
            return secret_string

    prefix = settings.__config__.prefix  # type: ignore

    def process_name(name: str):
        if settings.__config__.slugify_name:  # type: ignore
            name = slugify(name)
        return os.path.join(prefix, name)

    def produce_values():
        field_items = [name for name, _ in settings.__fields__.items()]
        explicit_secret_fields = settings.__config__.secret_fields  # type: ignore
        if explicit_secret_fields:
            field_items = [f for f in field_items if f in explicit_secret_fields]
        return {name: _get_secret(process_name(name)) for name in field_items}

    return produce_values()
