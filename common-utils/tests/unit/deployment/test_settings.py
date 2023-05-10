from common.settings import Settings, NestedSettings


def test_json():
    class NestedTest(NestedSettings):
        test_name: str

    class SettingsTest(Settings):
        test_json: NestedTest

    assert SettingsTest().test_json.dict() == {"test_name": "test_value"}


def test_single_value():
    class SettingsTest(Settings):
        test_single_value: str

    assert SettingsTest().test_single_value == "test"


def test_nested():
    class NestedTest(NestedSettings):
        test: str

    class SettingsTest(Settings):
        test_nested: NestedTest

    assert SettingsTest().test_nested.test == "test"
