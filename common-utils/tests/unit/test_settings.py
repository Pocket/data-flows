import os

import pytest
from common.settings import CommonSettings, NestedSettings, Settings


def test_json():
    "Assert that nested model works as expected"

    class NestedTest(NestedSettings):
        test_name: str

    class SettingsTest(Settings):
        test_json: NestedTest

    assert SettingsTest().test_json.dict() == {"test_name": "test_value"}  # type: ignore  # noqa: E501


def test_single_value():
    class SettingsTest(Settings):
        test_single_value: str

    assert SettingsTest().test_single_value == "test"  # type: ignore


def test_nested():
    class NestedTest(NestedSettings):
        test: str

    class SettingsTest(Settings):
        test_nested: NestedTest

    assert SettingsTest().test_nested.test == "test"  # type: ignore


CS_MAPPING = {
    "dev": (False, "dev"),
    "staging": (False, "dev"),
    "main": (True, "production"),
}


@pytest.mark.parametrize("deployment_type", ["dev", "staging", "main"])
def test_common_settings(deployment_type):
    cs = CommonSettings(deployment_type=deployment_type)
    assert cs.is_production is CS_MAPPING[deployment_type][0]
    assert cs.dev_or_production == CS_MAPPING[deployment_type][1]
    x = cs.deployment_type_value("dev", "staging", "main")
    assert x == deployment_type


@pytest.mark.parametrize("deployment_type", ["dev", "staging", "main"])
def test_common_settings_env(deployment_type):
    os.environ["DF_CONFIG_DEPLOYMENT_TYPE"] = deployment_type
    cs = CommonSettings() # type: ignore
    assert cs.is_production is CS_MAPPING[deployment_type][0]
    assert cs.dev_or_production == CS_MAPPING[deployment_type][1]
    x = cs.deployment_type_value("dev", "staging", "main")
    assert x == deployment_type
