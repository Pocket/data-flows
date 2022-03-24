from collections import defaultdict
import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from prefect import Flow, task, context
from prefect.executors import LocalDaskExecutor

import api_clients.braze.utils
from api_clients.braze import braze
from api_clients.braze.braze import BrazeClient
from api_clients.braze.utils import is_valid_email
from api_clients.prefect_key_value_store_client import get_kv, set_kv, format_key
from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from utils.iteration import chunks
from utils import config

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
FROM "STG_BRAZE_USER_DELTAS"
WHERE LOADED_AT > %(loaded_at_start)s
ORDER BY LOADED_AT ASC
LIMIT 1000 -- For debugging, limit to 1000.
"""


DEFAULT_TSTAMP_START = '2022-03-15'
LAST_LOADED_AT_KV_STORE_KEY = format_key(FLOW_NAME, "last_loaded_at")


@dataclass
class UserDelta:
    event_id: str
    loaded_at: datetime.datetime
    happened_at: datetime.datetime
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
        # Snowflake returns uppercase column names, and by convention we use lowercase variables in Python.
        return UserDelta(**{k.lower(): v for k, v in d.items()})


@task()
def get_extract_query_parameters():
    """
    :return: query parameters for EXTRACT_QUERY
    """
    query_params = {
        'loaded_at_start': get_kv(key=LAST_LOADED_AT_KV_STORE_KEY, default_value=DEFAULT_TSTAMP_START),
    }

    logger = context.get("logger")
    logger.info(f"extract query_params: {query_params}")

    return query_params


@task()
def set_last_loaded_at(user_deltas: List[UserDelta]):
    max_loaded_at = str(max(u.loaded_at for u in user_deltas))

    logger = context.get("logger")
    logger.info(f"Setting {LAST_LOADED_AT_KV_STORE_KEY} to {max_loaded_at}")

    set_kv(key=LAST_LOADED_AT_KV_STORE_KEY, value=max_loaded_at)


@task()
def get_user_deltas_from_dicts(dicts: List[Dict]) -> List[UserDelta]:
    return [UserDelta.from_dict(d) for d in dicts]


@task()
def filter_alias_only_signup_user_deltas(user_deltas: List[UserDelta]) -> List[UserDelta]:
    filtered_user_deltas = [u for u in user_deltas if u.new_user_identifier == 'email']
    logger = context.get("logger")
    logger.info(f"Found {len(filtered_user_deltas)}/{len(user_deltas)} alias-only signups")
    return filtered_user_deltas


@task()
def filter_user_deltas_with_new_pocket_user_emails(user_deltas: List[UserDelta]) -> List[UserDelta]:
    filtered_user_deltas = [u for u in user_deltas if u.is_new_email_alias_for_pocket_user]
    logger = context.get("logger")
    logger.info(f"Found {len(filtered_user_deltas)}/{len(user_deltas)} new aliases for Pocket users")
    return filtered_user_deltas


@task()
def filter_user_deltas_by_trigger(user_deltas: List[UserDelta], trigger: str) -> List[UserDelta]:
    filtered_user_deltas = [u for u in user_deltas if u.user_event_trigger == trigger]
    logger = context.get("logger")
    logger.info(f"Found {len(filtered_user_deltas)}/{len(user_deltas)} user deltas with trigger {trigger}")
    return filtered_user_deltas


@task()
def delete_user_profiles(users_to_delete: List[UserDelta]):
    """
    Deletes Braze user profiles

    Use cases:
    - If someone deletes their Pocket profile, their Braze user profile should also be deleted.
    """
    logger = context.get("logger")

    for chunk in chunks(users_to_delete, braze.USER_DELETE_LIMIT):
        logger.info(f"Deleting {len(chunk)} user profiles with event_id=[{chunk[0].event_id}..{chunk[-1].event_id}]")

        BrazeClient(logger=logger).delete_users(braze.UserDeleteInput(
            external_ids=[u.external_id for u in chunk]
        ))


@task()
def identify_users(user_deltas: List[UserDelta]):
    """
    Identifies a previously created alias-only user with an external id.

    Use cases:
    - If signs up on the Pocket Hits signup page, and then creates a Pocket account, these profiles should be linked.
    """
    logger = context.get("logger")

    for chunk in chunks(user_deltas, braze.IDENTIFY_USER_ALIAS_LIMIT):
        logger.info(f"Identifying {len(chunk)} user profiles with event_id=[{chunk[0].event_id}..{chunk[-1].event_id}]")

        BrazeClient(logger=logger).identify_users(braze.IdentifyUsersInput(
            aliases_to_identify=[
                braze.UserAliasIdentifier(
                    external_id=user.external_id,
                    user_alias=braze.UserAlias(
                        alias_label=braze.EMAIL_ALIAS_LABEL,
                        alias_name=user.email,
                    ),
                ) for user in chunk
            ]
        ))


@task()
def create_email_aliases(user_deltas: List[UserDelta]):
    """
    Creates aliases for users
    """
    logger = context.get("logger")

    for chunk in chunks(user_deltas, braze.NEW_USER_ALIAS_LIMIT):
        logger.info(f"Aliasing {len(chunk)} emails with event_id=[{chunk[0].event_id}..{chunk[-1].event_id}]")

        BrazeClient(logger=logger).create_new_user_aliases(braze.CreateUserAliasInput(
            user_aliases=[
                braze.UserAliasExternalIdAssociation(
                    external_id=user.external_id,
                    alias_label=braze.EMAIL_ALIAS_LABEL,
                    alias_name=user.email,
                ) for user in chunk
            ]
        ))


@task()
def map_newsletter_subscription_to_user_deltas(user_deltas: List[UserDelta]) -> Dict[str, List[UserDelta]]:
    """
    Maps subscription group names to user deltas
    :returns a dictionary where keys are subscription group names and values are a list user deltas for that name.
    """
    user_deltas_by_subscription_group_name = defaultdict(list)
    for user_delta in user_deltas:
        name = user_delta.subscribe_to_newsletter_subscription_group_name
        if name:
            user_deltas_by_subscription_group_name[name].append(user_delta)

    return user_deltas_by_subscription_group_name


@task()
def subscribe_users(user_deltas_by_subscription_group_name: Dict[str, List[UserDelta]]):
    """
    Subscribe users to a particular subscription group
    :param user_deltas_by_subscription_group_name: Maps subscription group names (POCKET_HITS_US_DAILY,
    POCKET_HITS_US_WEEKLY, POCKET_HITS_DE_DAILY) to UserDelta objects with users that should be subscribed to that group
    """
    logger = context.get("logger")
    logger.info(f"Newsletter subscription group signups for {','.join(user_deltas_by_subscription_group_name.keys())}")

    for subscription_group_name, subscription_group_user_deltas in user_deltas_by_subscription_group_name.items():
        for chunk in chunks(subscription_group_user_deltas, braze.SUBSCRIPTION_SET_LIMIT):
            logger.info(f"Subscribing {len(chunk)} users to {subscription_group_name}"
                        f" with event_id=[{chunk[0].event_id}..{chunk[-1].event_id}]")

            BrazeClient(logger=logger).subscribe_users(
                braze.SubscribeUsersInput(
                    subscription_group_id=braze.SUBSCRIPTION_GROUP_NAME_TO_ID[subscription_group_name],
                    subscription_state="subscribed",
                    external_id=[u.external_id for u in chunk if u.external_id is not None],  # Identified users
                    email=[u.email for u in chunk if u.external_id is None and is_valid_email(u.email)],  # Alias-only
                )
            )


@task()
def track_users(user_deltas: List[UserDelta]):
    """
    Updates

    If a user signs up for Pocket:
    - They should get a Braze user profile, such that they can receive emails for which they're eligible.
    - An event should be sent to Braze such that a double opt-in email can be sent to German users.

    :return:
    """
    logger = context.get("logger")

    for chunk in chunks(user_deltas, braze.USER_TRACK_LIMIT):
        logger.info(f"Tracking {len(chunk)} users with event_id=[{chunk[0].event_id}..{chunk[-1].event_id}]")

        # Note: The attributes and events below are bulk updates. They're not necessarily of equal length or in order.
        BrazeClient(logger=logger).track_users(braze.TrackUsersInput(
            attributes=get_attributes_for_user_deltas(chunk),
            events=get_events_for_user_deltas(chunk)
        ))


def get_attributes_for_user_deltas(user_deltas_chunk):
    return [
        braze.UserAttributes(
            external_id=user_delta.external_id,
            user_alias=braze.UserAlias('email', user_delta.email) if not user_delta.external_id else None,
            email=user_delta.email,
            is_premium=user_delta.is_premium,
            time_zone=user_delta.time_zone,
            country=user_delta.country,
        ) for user_delta in user_deltas_chunk
    ]


def get_events_for_user_deltas(user_deltas_chunk):
    return [
        braze.UserEvent(
            external_id=user_delta.external_id,
            user_alias=braze.UserAlias('email', user_delta.email) if not user_delta.external_id else None,
            name=user_delta.braze_event_name,
            properties=get_event_properties_for_user_delta(user_delta),
            time=api_clients.braze.utils.format_date(user_delta.happened_at),
        ) for user_delta in user_deltas_chunk
        if user_delta.braze_event_name
    ]


def get_event_properties_for_user_delta(user_delta):
    if user_delta.braze_event_name == 'newsletter_signup':
        return {
            'newsletter': user_delta.newsletter_signup_event_newsletter,
            'frequency': user_delta.newsletter_signup_event_frequency,
        }
    else:
        return {}


def _replace_email_domain(email: str, new_domain) -> str:
    return email.split('@')[0] + new_domain


@task()
def mask_email_domain_outside_production(rows: List[Dict], email_column='EMAIL'):
    """
    For debugging purposes, change the domain of all email addresses to '@example.com' unless we're in production.
    """
    if config.ENVIRONMENT != config.ENV_PROD:
        for row in rows:
            if row[email_column] is not None:
                row[email_column] = _replace_email_domain(row[email_column], new_domain='@example.com')

    return rows


with Flow(FLOW_NAME) as flow:
    user_deltas_dicts = PocketSnowflakeQuery()(
        query=EXTRACT_QUERY,
        data=get_extract_query_parameters(),
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_STAGING_SCHEMA,
        output_type=OutputType.DICT,
    )

    # Prevent users from getting emails by changing email domains outside the production environment to @example.com
    user_deltas_dicts = mask_email_domain_outside_production(user_deltas_dicts)

    # Convert Snowflake dicts to @dataclass objects.
    all_user_deltas = get_user_deltas_from_dicts(user_deltas_dicts)

    alias_only_email_results = create_email_aliases(
        filter_alias_only_signup_user_deltas(all_user_deltas)
    )

    # Apply attributes, events, and payments. This creates users with an external_id if they don't exist already.
    # The creation of user aliases for anonymous ('alias-only') signups needs to happen before track_users(),
    # because attributes can't be applies to alias-only users that don't exist yet.
    track_users_results = track_users(all_user_deltas, upstream_tasks=[alias_only_email_results])

    # Get the user deltas with a new email alias for a Pocket user (i.e. a user with an external_id)
    user_deltas_with_new_pocket_user_emails = filter_user_deltas_with_new_pocket_user_emails(all_user_deltas)

    # The creation of user aliases for Pocket accounts needs to happen after track_users, because track_users will
    # create profiles for new Pocket user signups.
    create_email_aliases_results = create_email_aliases(
        user_deltas_with_new_pocket_user_emails,
        upstream_tasks=[track_users_results]
    )

    # Identifying user profiles needs to happen after track_users, because the latter will create non-existing profiles.
    identify_users_results = identify_users(
        user_deltas_with_new_pocket_user_emails,
        upstream_tasks=[track_users_results]
    )

    # Subscribing users needs to happen after Pocket users and alias-only users have been created.
    subscribe_users_results = subscribe_users(
        map_newsletter_subscription_to_user_deltas(all_user_deltas),
        upstream_tasks=[track_users_results, alias_only_email_results]
    )

    # Deleting user profiles needs to happen after track_users, because the latter will create non-existing profiles.
    delete_users_results = delete_user_profiles(
        filter_user_deltas_by_trigger(all_user_deltas, trigger='account_delete'),
        upstream_tasks=[track_users_results]
    )

    # Set KV-store 'last_loaded_at' key to the maximum loaded_at seen so far if all tasks finished successfully.
    set_last_loaded_at(all_user_deltas, upstream_tasks=[
        alias_only_email_results,
        track_users_results,
        create_email_aliases_results,
        delete_users_results,
        subscribe_users_results,
    ])


if __name__ == "__main__":
    flow.run()
