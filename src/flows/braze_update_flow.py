from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from prefect import Flow, task, context

from api_clients import braze
from api_clients.braze import BrazeClient
from api_clients.prefect_key_value_store_client import get_kv, format_key
from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from utils.iteration import chunks

FLOW_NAME = Path(__file__).stem

EXTRACT_QUERY = """
SELECT
    EVENT_ID,
    ETL_TSTAMP,
    HAPPENED_AT,
    USER_EVENT_TRIGGER,
    EXTERNAL_ID,
    EMAIL,
    COUNTRY,
    TIME_ZONE,
    IS_PREMIUM,
    POCKET_LOCALE,
    SUBSCRIBE_TO_NEWSLETTER_SUBSCRIPTION_GROUP_ID,
    BRAZE_EVENT_NAME,
    NEWSLETTER_SIGNUP_EVENT_NEWSLETTER,
    NEWSLETTER_SIGNUP_EVENT_FREQUENCY
FROM "ANALYTICS"."DBT_MMIERMANS"."BRAZE_USER_DELTAS"
WHERE ETL_TSTAMP > %(tstamp_start)s
ORDER BY ETL_TSTAMP ASC
LIMIT 100 -- For debugging, limit to 100.
"""


DEFAULT_TSTAMP_START = '2022-03-10'


@dataclass
class UserDelta:
    event_id: str
    etl_tstamp: str
    happened_at: str
    user_event_trigger: str
    external_id: Optional[str]
    email: Optional[str]
    country: Optional[str]
    time_zone: Optional[str]
    is_premium: Optional[str]
    pocket_locale: Optional[str]
    subscribe_to_newsletter_subscription_group_id: Optional[str]
    braze_event_name: Optional[str]
    newsletter_signup_event_newsletter: Optional[str]
    newsletter_signup_event_frequency: Optional[str]

    @staticmethod
    def from_dict(d: Dict[str, Any]):
        return UserDelta(**{k.lower(): v for k, v in d.items()})


@task()
def get_extract_query_parameters():
    """
    :return: query parameters for EXTRACT_QUERY
    """
    query_params = {
        'tstamp_start': get_kv(key=format_key(FLOW_NAME, "last_etl"), default_value=DEFAULT_TSTAMP_START),
    }

    logger = context.get("logger")
    logger.info(f"extract query_params: {query_params}")

    return query_params


@task()
def get_user_deltas_from_dicts(dicts: List[Dict]) -> List[UserDelta]:
    return [UserDelta.from_dict(d) for d in dicts]


@task()
def filter_user_deltas(user_deltas: List[UserDelta], trigger: str) -> List[UserDelta]:
    filtered_user_deltas = [u for u in user_deltas if u.user_event_trigger == trigger]
    logger = context.get("logger")
    logger.info(f"{len(filtered_user_deltas)}/{len(user_deltas)} user deltas remain after '{trigger}' trigger filter")
    return filtered_user_deltas


@task()
def delete_user_profiles(users_to_delete: List[Dict]):
    """
    Deletes Braze user profiles

    Use cases:
    - If someone deletes their Pocket profile, their Braze user profile should also be deleted.
    """
    for account_signup_chunk in chunks(users_to_delete, braze.USER_DELETE_LIMIT):
        BrazeClient(logger=context.get("logger")).delete_users(braze.UserDeleteInput(
            external_ids=[u['EXTERNAL_ID'] for u in account_signup_chunk]
        ))


@task()
def create_email_aliases(user_deltas: List[UserDelta]):
    """
    Creates aliases for users
    """
    for user_deltas_chunk in chunks(user_deltas, braze.NEW_USER_ALIAS_LIMIT):
        BrazeClient(logger=context.get("logger")).create_new_user_aliases(braze.CreateUserAliasInput(
            user_aliases=[
                braze.UserAliasExternalIdAssociation(
                    external_id=user.external_id,
                    alias_label=braze.EMAIL_ALIAS_LABEL,
                    alias_name=user.email,
                ) for user in user_deltas_chunk
            ]
        ))


@task()
def track_users(user_deltas: List[Dict]):
    """
    Updates

    If a user signs up for Pocket:
    - They should get a Braze user profile, such that they can receive emails for which they're eligible.
    - An event should be sent to Braze such that a double opt-in email can be sent to German users.
    -

    :return:
    """
    for user_deltas_chunk in chunks(user_deltas, braze.USER_TRACK_LIMIT):
        BrazeClient(logger=context.get("logger")).track_users(braze.TrackUsersInput(
            attributes=[
                braze.UserAttributes(
                    external_id=account_signup['HASHED_USER_ID'],
                    is_premium=True,
                    date_of_first_session=braze.format_date(account_signup['CREATED_AT']),
                    time_zone=account_signup['TIMEZONE'],
                    country=account_signup['COUNTRY'],
                    home_city=account_signup['CITY'],
                    email_subscribe='subscribed'
                ) for account_signup in user_deltas_chunk
            ],
            events=[
                braze.UserEvent(
                    app_id=account_signup['BRAZE_API_ID'],
                    external_id=account_signup['HASHED_USER_ID'],
                    name="account_signup",
                    time=braze.format_date(account_signup['CREATED_AT']),
                ) for account_signup in user_deltas_chunk
            ]
        ))


@task()
def update_is_premium(account_signups: pd.DataFrame):
    """
    Updates whether a user is a premium user or not.

    Use cases:
    - Premium users should receive Pocket Hits without ads.
    """
    for account_signup_chunk in chunks(account_signups, braze.USER_TRACK_LIMIT):
        BrazeClient(logger=context.get("logger")).track_users(user_tracking=braze.TrackUsersInput(
            attributes=[
                braze.UserAttributes(
                    external_id=account_signup['HASHED_USER_ID'],
                    is_premium=True,
                ) for account_signup in account_signup_chunk
            ],
        ))


def _replace_email_domain(email: str, new_domain) -> str:
    return email.split('@')[0] + new_domain


@task()
def mask_email_domain(rows: List[Dict], email_column='EMAIL'):
    """
    For debugging purposes, change the domain of all email addresses to '@example.com'
    """
    for row in rows:
        if row[email_column] is not None:
            row[email_column] = _replace_email_domain(row[email_column], new_domain='@example.com')

    return rows


with Flow(FLOW_NAME) as flow:
    user_deltas_dicts = PocketSnowflakeQuery()(
        query=EXTRACT_QUERY,
        data=get_extract_query_parameters(),
        output_type=OutputType.DICT,
    )

    # Prevent real users from getting emails by masking the email domains in the development environment.
    user_deltas_dicts = mask_email_domain(user_deltas_dicts)

    # Convert Snowflake dicts to @dataclass objects.
    all_user_deltas = get_user_deltas_from_dicts(user_deltas_dicts)

    # Filter some subsets of users.
    anonymous_signup_user_deltas = filter_user_deltas(all_user_deltas, trigger='newsletter_signup')
    account_signup_user_deltas = filter_user_deltas(all_user_deltas, trigger='account_signup')

    # Create user aliases for anonymous ('alias-only') signups needs to happen before track_users(),
    # because attributes can't be applies to an alias-only user that doesn't exist yet.
    create_email_aliases(anonymous_signup_user_deltas)

    # Apply attributes, events, and payments. This creates users with an external_id if they don't exist already.
    track_users(all_user_deltas)

    # Create user aliases for Pocket account signups, which have an external_id.
    # Contrary to alias-only users, this needs to happen after track_users, because that step creates the user profile.
    create_email_aliases(account_signup_user_deltas)


if __name__ == "__main__":
    flow.run()
