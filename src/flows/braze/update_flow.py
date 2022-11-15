from collections import defaultdict
import datetime
from dataclasses import dataclass
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

from prefect import Flow, task, context, Parameter
from prefect.executors import LocalDaskExecutor

from api_clients.braze import models
from api_clients.braze.client import (
    BrazeClient,
    IDENTIFY_USER_ALIAS_LIMIT,
    NEW_USER_ALIAS_LIMIT,
    SUBSCRIPTION_SET_LIMIT,
    USER_TRACK_LIMIT,
    USER_DELETE_LIMIT,
)
from api_clients.braze.pocket_config import EMAIL_ALIAS_LABEL, SUBSCRIPTION_GROUP_NAME_TO_ID
from api_clients.braze.utils import is_valid_email, format_date
from api_clients.prefect_key_value_store_client import get_kv, set_kv, format_key
from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from common_tasks.braze import mask_email_domain_outside_production
from utils.flow import get_flow_name, get_s3_result
from utils.iteration import chunks
from utils import config
from common_tasks.mapping import split_in_chunks, split_dict_of_lists_in_chunks

FLOW_NAME = get_flow_name(__file__)

EXTRACT_QUERY = """
SELECT
    EVENT_ID,
    LOADED_AT,
    HAPPENED_AT,
    USER_EVENT_TRIGGER,
    EXTERNAL_ID,
    EMAIL,
    IS_NEW_EMAIL_ALIAS_FOR_POCKET_USER AS HAS_EMAIL,
    IS_PREMIUM,
    TIME_ZONE,
    COUNTRY,
    POCKET_LOCALE,
    SUBSCRIBE_TO_NEWSLETTER_SUBSCRIPTION_GROUP_NAME,
    EMAIL_SUBSCRIBE,
    BRAZE_EVENT_NAME,
    NEWSLETTER_SIGNUP_EVENT_NEWSLETTER,
    NEWSLETTER_SIGNUP_EVENT_FREQUENCY
FROM "{table_name}"
WHERE LOADED_AT > %(loaded_at_start)s
ORDER BY LOADED_AT ASC
"""


DEFAULT_LOADED_AT_START = '2022-03-22'  # Value to use for the loaded_at_start query param if KV-store key is missing.
LAST_LOADED_AT_KV_STORE_KEY = format_key(FLOW_NAME, "last_loaded_at")  # KV-store key name
DEFAULT_MAX_OPERATIONS_PER_TASK_RUN = 100000  # The workload is run in parallel in chunks of this many rows.
DEFAULT_TABLE_NAME = 'STG_BRAZE_USER_DELTAS'


@dataclass
class UserDelta:
    """
    This class corresponds to the query result from EXTRACT_QUERY.
    """

    event_id: str
    loaded_at: datetime.datetime
    happened_at: datetime.datetime
    user_event_trigger: str
    """Unique Braze user identifier (based on the Pocket hashed user id)"""
    external_id: Optional[str]
    email: Optional[str]
    """If true, an email alias will be created for the external_id"""
    has_email: Optional[bool]
    is_premium: Optional[bool]
    country: Optional[str]
    time_zone: Optional[str]
    pocket_locale: Optional[str]
    subscribe_to_newsletter_subscription_group_name: Optional[str]
    email_subscribe: Optional[str]
    braze_event_name: Optional[str]
    newsletter_signup_event_newsletter: Optional[str]
    newsletter_signup_event_frequency: Optional[str]

    @staticmethod
    def from_dict(d: Dict[str, Any]):
        # Snowflake returns uppercase column names, and we use lowercase variables in Python.
        return UserDelta(**{k.lower(): v for k, v in d.items()})


@task()
def prepare_extract_query_and_parameters(table_name: str) -> Tuple[str, Dict]:
    """
    :return: Tuple where the first element is the query string, and the second the query parameters
    """
    # Table name can only contain alphanumeric characters and underscores to prevent SQL-injection.
    assert re.fullmatch(r'[a-zA-Z0-9_]+', table_name), "Invalid table name"
    query = EXTRACT_QUERY.format(table_name=table_name)

    query_params = {
        'loaded_at_start': get_kv(key=LAST_LOADED_AT_KV_STORE_KEY, default_value=DEFAULT_LOADED_AT_START),
    }

    logger = context.get("logger")
    logger.info(f"extract query_params: {query_params}")

    return query, query_params


@task()
def set_last_loaded_at(user_deltas: List[UserDelta]):
    """
    Update the KV-store value for the loaded_at_start query parameter to the maximum loaded_at value in user_deltas.
    :param user_deltas:
    """
    logger = context.get("logger")
    if user_deltas:
        max_loaded_at = str(max(u.loaded_at for u in user_deltas))
        logger.info(f"Setting {LAST_LOADED_AT_KV_STORE_KEY} to {max_loaded_at}")
        set_kv(key=LAST_LOADED_AT_KV_STORE_KEY, value=max_loaded_at)
    else:
        logger.info(f"{LAST_LOADED_AT_KV_STORE_KEY} is not updated because we did not query any rows from Snowflake.")


@task()
def get_user_deltas_from_dicts(dicts: List[Dict]) -> List[UserDelta]:
    """
    Converts a dicts to UserDelta objects
    :param dicts: Dictionary where keys match attributes in UserDelta (case-insensitive)
    :return: List of UserDelta objects
    """
    return [UserDelta.from_dict(d) for d in dicts]


@task()
def filter_user_deltas_with_email(user_deltas: List[UserDelta]) -> List[UserDelta]:
    """
    :param user_deltas:
    :return: The subset of user_deltas where a user signed up with or changed their email address.
    """
    filtered_user_deltas = [u for u in user_deltas if u.has_email]
    logger = context.get("logger")
    logger.info(f"Found {len(filtered_user_deltas)}/{len(user_deltas)} new aliases for Pocket users")
    return filtered_user_deltas


@task()
def filter_user_deltas_by_trigger(user_deltas: List[UserDelta], trigger: str) -> List[UserDelta]:
    """
    :param user_deltas:
    :param trigger:
    :return: The subset of user_deltas where the user_event_trigger attribute matches `trigger`.
    """
    filtered_user_deltas = [u for u in user_deltas if u.user_event_trigger == trigger]
    logger = context.get("logger")
    logger.info(f"Found {len(filtered_user_deltas)}/{len(user_deltas)} user deltas with trigger {trigger}")
    return filtered_user_deltas


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=2))
def delete_user_profiles(users_to_delete: List[UserDelta]):
    """
    Deletes Braze user profiles

    Use cases:
    - If someone deletes their Pocket profile, their Braze user profile should also be deleted.
    """
    logger = context.get("logger")

    for chunk in chunks(users_to_delete, USER_DELETE_LIMIT):
        logger.info(f"Deleting {len(chunk)} user profiles with event_ids=[{[c.event_id for c in chunk]}]")

        BrazeClient(logger=logger).delete_users(models.UserDeleteInput(
            external_ids=[u.external_id for u in chunk],
        ))


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=2))
def identify_users(user_deltas: List[UserDelta]):
    """
    Identifies a previously created alias-only user with an external id.

    Use cases:
    - If signs up on the Pocket Hits signup page, and then creates a Pocket account, these profiles should be linked.
    """
    logger = context.get("logger")

    for chunk in chunks(user_deltas, IDENTIFY_USER_ALIAS_LIMIT):
        logger.info(f"Identifying {len(chunk)} user profiles with event_ids=[{[c.event_id for c in chunk]}]")

        BrazeClient(logger=logger).identify_users(models.IdentifyUsersInput(
            aliases_to_identify=[
                models.UserAliasIdentifier(
                    external_id=user.external_id,
                    user_alias=models.UserAlias(
                        alias_label=EMAIL_ALIAS_LABEL,
                        alias_name=user.email,
                    ),
                ) for user in chunk
            ]
        ))


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=2))
def create_email_aliases(user_deltas: List[UserDelta]):
    """
    Creates aliases for users
    """
    logger = context.get("logger")

    for chunk in chunks(user_deltas, NEW_USER_ALIAS_LIMIT):
        logger.info(f"Aliasing {len(chunk)} emails with event_ids=[{[c.event_id for c in chunk]}]")

        BrazeClient(logger=logger).create_new_user_aliases(models.CreateUserAliasInput(
            user_aliases=[
                models.UserAliasExternalIdAssociation(
                    external_id=user.external_id,
                    alias_label=EMAIL_ALIAS_LABEL,
                    alias_name=user.email,
                ) for user in chunk
            ]
        ))


@task()
def group_user_deltas_by_newsletter_subscription_name(user_deltas: List[UserDelta]) -> Dict[str, List[UserDelta]]:
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


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=2))
def subscribe_users(subscription_group_user_deltas: Tuple[str, List[UserDelta]]):
    """
    Subscribe users to a particular subscription group
    :param subscription_group_user_deltas: Maps subscription group names (POCKET_HITS_US_DAILY,
    POCKET_HITS_US_WEEKLY, POCKET_HITS_DE_DAILY) to UserDelta objects with users that should be subscribed to that group
    """
    logger = context.get("logger")

    subscription_group_name, user_deltas = subscription_group_user_deltas
    for chunk in chunks(user_deltas, SUBSCRIPTION_SET_LIMIT):
        logger.info(f"Subscribing {len(chunk)} users to {subscription_group_name}"
                    f" with event_ids=[{[c.event_id for c in chunk]}]")

        BrazeClient(logger=logger).subscribe_users(
            models.SubscribeUsersInput(
                subscription_group_id=SUBSCRIPTION_GROUP_NAME_TO_ID[subscription_group_name],
                subscription_state="subscribed",
                external_id=[u.external_id for u in chunk if u.external_id is not None],  # Identified users
                email=[u.email for u in chunk if u.external_id is None and is_valid_email(u.email)],  # Alias-only
            )
        )


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=2))
def track_users(user_deltas: List[UserDelta]):
    """
    Sends attributes and events to Braze based on UserDelta objects. Also creates new users who have an external_id.
    :param user_deltas List of user_deltas
    """
    logger = context.get("logger")

    for chunk in chunks(user_deltas, USER_TRACK_LIMIT):
        logger.info(f"Tracking {len(chunk)} users with event_ids=[{[c.event_id for c in chunk]}]")

        # Note: The attributes and events below are bulk updates. They're not necessarily of equal length or in order.
        BrazeClient(logger=logger).track_users(models.TrackUsersInput(
            attributes=get_attributes_for_user_deltas(chunk),
            events=get_events_for_user_deltas(chunk)
        ))


def get_attributes_for_user_deltas(user_deltas: Sequence[UserDelta]) -> List[models.UserAttributes]:
    """
    :param user_deltas:
    :return: Braze user attributes based the given user_deltas. len(returned list) <= len(user_deltas).
    """
    return [
        models.UserAttributes(
            external_id=user_delta.external_id,
            user_alias=models.UserAlias(EMAIL_ALIAS_LABEL, user_delta.email) if not user_delta.external_id else None,
            _update_existing_only=True,  # Do not create new profiles if one does not exist.
            email=user_delta.email,
            is_premium=user_delta.is_premium,
            time_zone=user_delta.time_zone,
            country=user_delta.country,
            pocket_locale=user_delta.pocket_locale,
            email_subscribe=user_delta.email_subscribe,
        ) for user_delta in user_deltas
    ]


def get_events_for_user_deltas(user_deltas: Sequence[UserDelta]) -> List[models.UserEvent]:
    """
    :param user_deltas:
    :return: Braze custom events based the given user_deltas. len(returned list) <= len(user_deltas).
    """
    return [
        models.UserEvent(
            external_id=user_delta.external_id,
            user_alias=models.UserAlias('email', user_delta.email) if not user_delta.external_id else None,
            name=user_delta.braze_event_name,
            properties=get_event_properties_for_user_delta(user_delta),
            time=format_date(user_delta.happened_at),
        ) for user_delta in user_deltas
        if user_delta.braze_event_name
    ]


def get_event_properties_for_user_delta(user_delta: UserDelta) -> models.EventPropertiesType:
    """
    Format the Braze event properties for a custom event.
    Brace doc with accepted types: https://www.braze.com/docs/api/objects_filters/event_object/#event-properties-object
    Pocket events: https://docs.google.com/spreadsheets/d/1HIR33seaMDh55vQnNxDJsKXI6I7Afku9gxHMuY7CbrM/edit#gid=41936054
    :param user_delta:
    :return:
    """
    if user_delta.braze_event_name == 'newsletter_signup':
        return {
            'newsletter': user_delta.newsletter_signup_event_newsletter,
            'frequency': user_delta.newsletter_signup_event_frequency,
        }
    else:
        return {}

with Flow(FLOW_NAME, executor=LocalDaskExecutor(), result=get_s3_result()) as flow:
    # To backfill data we can manually run this flow and override the Snowflake database, schema, and table.
    # The default is ANALYTICS.DBT_STAGING.STG_BRAZE_USER_DELTAS.
    snowflake_database = Parameter('snowflake_database', default=config.SNOWFLAKE_ANALYTICS_DATABASE)
    snowflake_schema = Parameter('snowflake_schema', default=config.SNOWFLAKE_ANALYTICS_DBT_STAGING_SCHEMA)
    extract_query_table_name = Parameter('snowflake_table_name', default=DEFAULT_TABLE_NAME)
    # This parameter controls the number of rows processed by each task run. Higher number = less parallelism.
    max_operations_per_task_run = Parameter('max_operations_per_task_run', default=DEFAULT_MAX_OPERATIONS_PER_TASK_RUN)

    extract_query, extract_query_params = prepare_extract_query_and_parameters(table_name=extract_query_table_name)

    user_deltas_dicts = PocketSnowflakeQuery()(
        query=extract_query,
        data=extract_query_params,
        database=snowflake_database,
        schema=snowflake_schema,
        output_type=OutputType.DICT,
    )

    # Prevent us from accidentally emailing users from our dev environment by changing all domains to @example.com,
    # unless we are in the production environment.
    # Be aware: Sending a large number of emails to fake (@example.com) accounts can harm our email reputation score.
    user_deltas_dicts = mask_email_domain_outside_production(user_deltas_dicts)

    # Convert Snowflake dicts to @dataclass objects.
    all_user_deltas = get_user_deltas_from_dicts(user_deltas_dicts)

    # Get the user deltas that contain an email
    user_deltas_with_email = filter_user_deltas_with_email(all_user_deltas)

    # Create an email only profile,
    # or add an email alias to an existing user profile,
    # for our deltas that contain an email
    create_email_aliases_task = create_email_aliases.map(
        split_in_chunks(
            user_deltas_with_email,
            chunk_size=max_operations_per_task_run,
        ),
    )

    # Identify ('merge') Pocket users with their email alias any time we have a delta with an email address
    # This deletes their old email alias.
    identify_users_task = identify_users.map(
        split_in_chunks(
            user_deltas_with_email,
            chunk_size=max_operations_per_task_run,
        ),
    ).set_upstream(
        create_email_aliases_task,  # Users creation needs to happen first
    )

    # Subscribing users needs to happen after Pocket users and alias-only users have been created.
    subscribe_users_task = subscribe_users.map(
        split_dict_of_lists_in_chunks(
           group_user_deltas_by_newsletter_subscription_name(all_user_deltas),
           chunk_size=max_operations_per_task_run,
        )
    ).set_upstream(
        identify_users_task,  # Merging of users needs to happen first
    )

    # Apply attributes, events, and payments.
    # Note: Both external_id and email aliases cannot be set simultaneously for the same user in the same call
    track_users_task = track_users.map(
        split_in_chunks(all_user_deltas, chunk_size=max_operations_per_task_run)
    ).set_upstream(
        subscribe_users_task   # Subscribing users needs to happen first
    )

    # Deleting user profiles needs to happen after all of our other calls,
    # because it is possible the latter will create non-existing profiles.
    delete_users_results = delete_user_profiles.map(
        split_in_chunks(
            filter_user_deltas_by_trigger(all_user_deltas, trigger='account_delete'),
            chunk_size=max_operations_per_task_run,
        ),
    ).set_upstream(
        track_users_task,  # Users creation needs to happen first, because otherwise deleted users might be recreated.
    )

    # Set KV-store 'last_loaded_at' key to the maximum loaded_at seen so far if all tasks finished successfully.
    set_last_loaded_at(all_user_deltas).set_dependencies(upstream_tasks=[
        create_email_aliases_task,
        identify_users_task,
        subscribe_users_task,
        track_users_task,
        delete_users_results,
    ])


if __name__ == "__main__":
    flow.run()
