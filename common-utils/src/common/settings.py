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
    project_name: str = Field(
        ...,
        description=(
            "Data flows project name."
        ),
    )
