from pydantic import BaseModel, BaseSettings


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
