from typing import Literal

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


def get_deployment_type_setting(
    dev: str = "no-value", staging: str = "no-value", main: str = "no-value"
) -> str:
    """Helper settings function to create dynamic FlowSpec attributes.

    Args:
        dev (str, optional):
        Value to return if deployment type is "dev". Defaults to "no-value".
        staging (str, optional):
        Value to return if deployment type is "staging". Defaults to "no-value".
        main (str, optional):
        Value to return if deployment type is "main". Defaults to "no-value".

    Returns:
        str: _description_
    """
    cs = CommonSettings()
    kv_pairs = {"dev": dev, "staging": staging, "main": main}
    return str(kv_pairs[cs.deployment_type])
