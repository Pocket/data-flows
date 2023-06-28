import os

from common.settings import CommonSettings, NestedSettings, Settings


def test_json():
    "Assert that nested model works as expected"

    class NestedTest(NestedSettings):
        test_name: str

    class SettingsTest(Settings):
        test_json: NestedTest

    assert SettingsTest().test_json.dict() == {"test_name": "test_value"}  # type: ignore


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


def test_common_settings():
    cs = CommonSettings()  # type: ignore
    if x := os.getenv("DF_CONFIG_DEPLOYMENT_TYPE"):
        assert x == cs.deployment_type
    cs.deployment_type = "dev"
    assert cs.deployment_type == "dev"
    assert cs.is_production is False
    assert cs.dev_or_production == "dev"
    x = cs.deployment_type_value("dev", "staging", "main")
    assert x == "dev"


def test_common_settings_not_dev():
    cs = CommonSettings()  # type: ignore
    cs.deployment_type = "main"
    assert cs.is_production is True
    assert cs.dev_or_production == "production"
    x = cs.deployment_type_value("dev", "staging", "main")
    assert x == "main"
    assert x == "main"
