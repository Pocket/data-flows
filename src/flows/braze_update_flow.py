from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
from prefect import Flow, task, context

from api_clients import braze
from api_clients.braze import BrazeClient
from api_clients.prefect_key_value_store_client import get_kv, format_key
from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from utils.iteration import chunks
from utils.config import ENVIRONMENT, ENV_PROD

FLOW_NAME = Path(__file__).stem

EXTRACT_QUERY = """
SELECT
    EVENT_ID,
    LOADED_AT,
    HAPPENED_AT,
    USER_EVENT_TRIGGER,
    EXTERNAL_ID,
    EMAIL,
    NEW_USER_IDENTIFIER,
    IS_NEW_EMAIL_ALIAS_FOR_POCKET_USER,
    IS_PREMIUM,
    TIME_ZONE,
    COUNTRY,
    POCKET_LOCALE,
    SUBSCRIBE_TO_NEWSLETTER_SUBSCRIPTION_GROUP_NAME,
    BRAZE_EVENT_NAME,
    NEWSLETTER_SIGNUP_EVENT_NEWSLETTER,
    NEWSLETTER_SIGNUP_EVENT_FREQUENCY
FROM "ANALYTICS"."DBT_MMIERMANS_STAGING"."STG_BRAZE_USER_DELTAS" -- TODO: Change database to production or make it configurable?
WHERE LOADED_AT > %(tstamp_start)s
AND USER_EVENT_TRIGGER = 'newsletter_signup' -- TODO: Remove debug code
ORDER BY LOADED_AT ASC
LIMIT 10 -- For debugging, limit to 100.
"""


DEFAULT_TSTAMP_START = '2022-03-15'


@dataclass
class UserDelta:
    event_id: str
    loaded_at: str
    happened_at: str
    user_event_trigger: str
    """Unique Braze user identifier (based on the Pocket hashed user id)"""
    external_id: Optional[str]
    email: Optional[str]
    """
    - If set to 'email', create an alias-only Braze user based on the email address.
    - If set to 'external_id', create an identified Braze user based on external_id.
    - Otherwise, no new user profile needs to be created in Braze.
    """
    new_user_identifier: Optional[str]
    """If true, an email alias will be created for the external_id"""
    is_new_email_alias_for_pocket_user: Optional[bool]
    is_premium: Optional[bool]
    country: Optional[str]
    time_zone: Optional[str]
    pocket_locale: Optional[str]
    subscribe_to_newsletter_subscription_group_name: Optional[str]
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
def get_alias_only_signup_deltas(user_deltas: List[UserDelta]) -> List[UserDelta]:
    filtered_user_deltas = [u for u in user_deltas if u.new_user_identifier == 'email']
    logger = context.get("logger")
    logger.info(f"Found {len(filtered_user_deltas)}/{len(user_deltas)} alias-only signups")
    return filtered_user_deltas


@task()
def get_new_email_aliases_for_pocket_users(user_deltas: List[UserDelta]) -> List[UserDelta]:
    filtered_user_deltas = [u for u in user_deltas if u.is_new_email_alias_for_pocket_user]
    logger = context.get("logger")
    logger.info(f"Found {len(filtered_user_deltas)}/{len(user_deltas)} new aliases for Pocket users")
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
def create_alias_only_email_users(user_deltas: List[UserDelta]):
    create_email_aliases(get_alias_only_signup_deltas(user_deltas))


@task()
def create_email_aliases_for_pocket_users(user_deltas: List[UserDelta]):
    create_email_aliases(get_new_email_aliases_for_pocket_users(user_deltas))


@task()
def track_users(user_deltas: List[UserDelta]):
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
                    external_id=user_delta.external_id,
                    user_alias=braze.UserAlias('email', user_delta.email) if not user_delta.external_id else None,
                    is_premium=user_delta.is_premium,
                    date_of_first_session=braze.format_date(user_delta.happened_at),
                    time_zone=user_delta.time_zone,
                    country=user_delta.country,
                ) for user_delta in user_deltas_chunk
            ],
            # events=[
            #     braze.UserEvent(
            #         app_id=account_signup['BRAZE_API_ID'],
            #         external_id=account_signup['HASHED_USER_ID'],
            #         name="account_signup",
            #         time=braze.format_date(account_signup['CREATED_AT']),
            #     ) for account_signup in user_deltas_chunk
            # ]
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
def mask_email_domain_outside_production(rows: List[Dict], email_column='EMAIL'):
    """
    For debugging purposes, change the domain of all email addresses to '@example.com' unless we're in production.
    """
    if ENVIRONMENT != ENV_PROD:
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

    # Prevent users from getting emails by changing email domains outside the production environment to @example.com
    user_deltas_dicts = mask_email_domain_outside_production(user_deltas_dicts)

    # Convert Snowflake dicts to @dataclass objects.
    all_user_deltas = get_user_deltas_from_dicts(user_deltas_dicts)

    alias_only_email_results = create_alias_only_email_users(all_user_deltas)

    # Apply attributes, events, and payments. This creates users with an external_id if they don't exist already.
    track_users(all_user_deltas)
    # The creation of user aliases for anonymous ('alias-only') signups needs to happen before track_users(),
    # because attributes can't be applies to alias-only users that don't exist yet.
    alias_only_email_results.set_upstream(track_users, upstream_tasks=[alias_only_email_results])

    create_email_aliases_for_pocket_users(all_user_deltas)
    # The creation of user aliases for Pocket account signups needs to happen after track_users(), because these users
    # have an external_id and are created through the track_users() call.
    flow.set_dependencies(create_email_aliases_for_pocket_users, upstream_tasks=[track_users])



if __name__ == "__main__":
    flow.run()
