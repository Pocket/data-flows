from typing import Literal, Union

from pydantic import BaseModel, BaseSettings, Field


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
