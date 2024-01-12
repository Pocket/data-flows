import datetime
import re


def format_date(dt: datetime.datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def is_valid_email(email: str) -> bool:
    """
    Validates an email address based on what /subscription/status/set endpoint checks for

    Based on trial-and-error this is: the email must looks like foo@bar.com,
    where 'foo', 'bar', and 'com' are non-empty
    strings. The subscription endpoint doesn't check for invalid characters: '!@%.&'
    is currently accepted as an email.

    Braze does document a more sophisticated email validation, but the subscription API
    endpoint doesn't use this:
    https://www.braze.com/docs/user_guide/onboarding_with_braze/email_setup/email_validation/#host-part-validation-rules

    :param email:
    :return: True if the email is valid
    """
    return re.match(r".+@.+\..+", email) is not None
