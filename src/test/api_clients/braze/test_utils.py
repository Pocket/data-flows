from api_clients.braze.utils import is_valid_email


def test_is_valid_email():
    assert is_valid_email("jane@example.com")

    # Invalid emails
    assert not is_valid_email("@.com")
    assert not is_valid_email("jane@com")
    assert not is_valid_email("jan@.com")
    assert not is_valid_email("jane.com")
    assert not is_valid_email("@example.com")
    assert not is_valid_email("jane@example")
    assert not is_valid_email("jane@example.")
