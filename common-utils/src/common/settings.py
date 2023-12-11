import os
from functools import cache
from typing import Literal, Union

from common.cloud.aws_utils import fetch_aws_secret
from pydantic import BaseModel, BaseSettings, Field

# allow for setting the secrets manager service via envar
SECRETS_MANAGER = os.getenv("DF_CONFIG_SECRETS_MANAGER", "aws")

SECRETS_MANAGER_CONFIG = {"aws": fetch_aws_secret}


class NestedSettings(BaseModel):
    """Base nested settings model to use for defining
    nested settings keys.  This allows for some enforcement
    of expectations.
    """

    class Config:
        validate_assignment = True


class Settings(BaseSettings):
    """Base settings model for definiting settings.
    This allows for some enforcement of expectations.
    """

    class Config:
        env_prefix = "df_config_"
        env_nested_delimiter = "__"


class CommonSettings(Settings):
    """Base Settings Model for common settings that are available
    to all flows.
    """

    deployment_type: Literal["dev", "staging", "main"] = Field(
        "dev",
        description=(
            "Deployment type for execution environments. Defaults to 'dev'. "
            "Deployment type is used in Prefect object names to infer environment. "
        ),
    )

    @property
    def is_production(self) -> bool:
        """Returns True if deployment type is 'main'.
        Deployment types of staging and dev will
        return false.

        Returns:
            bool: Whether the current deployment type is considered Production.
        """
        return self.deployment_type == "main"

    @property
    def dev_or_production(self) -> str:
        """Shortcut to get an explicit
        dev' or 'production'.

        Returns:
            str: 'dev' or 'production'
        """
        answer = "dev"
        if self.is_production:
            answer = "production"
        return answer

    def deployment_type_value(
        self,
        dev: Union[str, None] = None,
        staging: Union[str, None] = None,
        main: Union[str, None] = None,
    ) -> Union[str, None]:
        mapping = {"dev": dev, "staging": staging, "main": main}
        return mapping[self.deployment_type]


CS = CommonSettings()  # type: ignore


class SecretSettings(Settings):
    class Config:
        """Custom Config class that can be used to enable
        secrets fetching from supported service enabled via
        DF_CONFIG_SECRETS_MANAGER envar.
        """

        secrets_manager = SECRETS_MANAGER_CONFIG[SECRETS_MANAGER]
        secret_fields = []
        prefix = f"data-flows/{CS.deployment_type}"
        slugify_name = True

        @classmethod
        def customise_sources(
            cls,
            init_settings,
            env_settings,
            file_secret_settings,
        ):
            return (
                init_settings,
                env_settings,
                file_secret_settings,
                cls.secrets_manager,  # type: ignore
            )
<<<<<<< HEAD


@cache
def get_cached_settings(settings: Settings):
    return settings()  # type: ignore
=======
>>>>>>> 185a9b9 (feat: adding logic for secrets manager based settings)
