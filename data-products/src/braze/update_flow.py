import datetime
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from common.databases.snowflake_utils import MozSnowflakeConnector
from common.deployment.worker import FlowDeployment, FlowSpec
from prefect import flow, get_run_logger, task
from prefect_snowflake.database import snowflake_query
from shared.api_clients.braze import models
from shared.api_clients.braze.client import (
    IDENTIFY_USER_ALIAS_LIMIT,
    NEW_USER_ALIAS_LIMIT,
    SUBSCRIPTION_SET_LIMIT,
    USER_DELETE_LIMIT,
    USER_TRACK_LIMIT,
    BrazeClient,
)
from shared.api_clients.braze.pocket_config import (
    CS,
    EMAIL_ALIAS_LABEL,
    SUBSCRIPTION_GROUP_NAME_TO_ID,
)
from shared.api_clients.braze.utils import format_date, is_valid_email
from shared.iteration_utils import chunks
from shared.offset_utils import get_last_offset, upsert_new_offset
from shared.tasks import split_dict_of_lists_in_chunks, split_in_chunks
from snowflake.connector import DictCursor

OFFSET_KEY = "update_braze"

GET_OFFSET_QUERY = f"""select coalesce(any_value(last_offset), '2022-03-22') as last_offset
    from sql_offset_state
    where sql_folder_name = '{OFFSET_KEY}'"""

UPDATE_OFFSET_SQL = f"""
    merge into sql_offset_state dt using (
    select '{OFFSET_KEY}' as sql_folder_name, 
    current_timestamp as created_at, 
    current_timestamp as updated_at,
    %(new_offset)s as last_offset
    ) st on st.sql_folder_name = dt.sql_folder_name
    when matched then update 
    set updated_at = st.updated_at,
        last_offset = st.last_offset
    when not matched then insert (sql_folder_name, created_at, updated_at, last_offset) 
    values (st.sql_folder_name, st.created_at, st.updated_at, st.last_offset)"""

EXTRACT_QUERY = """
SELECT
    EVENT_ID,
    LOADED_AT,
    HAPPENED_AT,
    USER_EVENT_TRIGGER,
    EXTERNAL_ID,
    {email_expression},
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
FROM {table_name}
WHERE LOADED_AT > %(loaded_at_start)s
ORDER BY LOADED_AT ASC
{limit}
"""


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
def get_last_loaded_at(user_deltas: List[UserDelta]) -> str | None:
    """
    Return new offset or None.
    :param user_deltas:
    """
    logger = get_run_logger()
    max_loaded_at = None
    if user_deltas:
        max_loaded_at = str(max(u.loaded_at for u in user_deltas))
        logger.info(f"Returning new offset as {max_loaded_at}")
    else:
        logger.info("New offset is not updated because we did not query any rows.")
    return max_loaded_at


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
    logger = get_run_logger()
    logger.info(
        f"Found {len(filtered_user_deltas)}/{len(user_deltas)} new aliases for Pocket users"
    )
    return filtered_user_deltas


@task()
def filter_user_deltas_by_trigger(
    user_deltas: List[UserDelta], trigger: str
) -> List[UserDelta]:
    """
    :param user_deltas:
    :param trigger:
    :return: The subset of user_deltas where the user_event_trigger attribute matches `trigger`.
    """
    filtered_user_deltas = [u for u in user_deltas if u.user_event_trigger == trigger]
    logger = get_run_logger()
    logger.info(
        f"Found {len(filtered_user_deltas)}/{len(user_deltas)} user deltas with trigger {trigger}"
    )
    return filtered_user_deltas


@task(retries=3, retry_delay_seconds=30)
def delete_user_profiles(users_to_delete: List[UserDelta]):
    """
    Deletes Braze user profiles

    Use cases:
    - If someone deletes their Pocket profile, their Braze user profile should also be deleted.
    """
    logger = get_run_logger()

    for chunk in chunks(users_to_delete, USER_DELETE_LIMIT):
        logger.info(
            f"Deleting {len(chunk)} user profiles with event_ids=[{[c.event_id for c in chunk]}]"
        )

        BrazeClient(logger=logger).delete_users(
            models.UserDeleteInput(
                external_ids=[u.external_id for u in chunk],
            )
        )


@task(retries=3, retry_delay_seconds=30)
def identify_users(user_deltas: List[UserDelta]):
    """
    Identifies a previously created alias-only user with an external id.

    Use cases:
    - If signs up on the Pocket Hits signup page, and then creates a Pocket account, these profiles should be linked.
    """
    logger = get_run_logger()

    for chunk in chunks(user_deltas, IDENTIFY_USER_ALIAS_LIMIT):
        logger.info(
            f"Identifying {len(chunk)} user profiles with event_ids=[{[c.event_id for c in chunk]}]"
        )

        BrazeClient(logger=logger).identify_users(
            models.IdentifyUsersInput(
                aliases_to_identify=[
                    models.UserAliasIdentifier(
                        external_id=user.external_id,
                        user_alias=models.UserAlias(
                            alias_label=EMAIL_ALIAS_LABEL,
                            alias_name=user.email,
                        ),
                    )
                    for user in chunk
                ]
            )
        )


@task(retries=3, retry_delay_seconds=30)
def create_email_aliases(user_deltas: List[UserDelta]):
    """
    Creates aliases for users
    """
    logger = get_run_logger()

    for chunk in chunks(user_deltas, NEW_USER_ALIAS_LIMIT):
        logger.info(
            f"Aliasing {len(chunk)} emails with event_ids=[{[c.event_id for c in chunk]}]"
        )

        BrazeClient(logger=logger).create_new_user_aliases(
            models.CreateUserAliasInput(
                user_aliases=[
                    models.UserAliasExternalIdAssociation(
                        external_id=user.external_id,
                        alias_label=EMAIL_ALIAS_LABEL,
                        alias_name=user.email,
                    )
                    for user in chunk
                ]
            )
        )


@task()
def group_user_deltas_by_newsletter_subscription_name(
    user_deltas: List[UserDelta],
) -> Dict[str, List[UserDelta]]:
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


@task(retries=3, retry_delay_seconds=30)
def subscribe_users(subscription_group_user_deltas: Tuple[str, List[UserDelta]]):
    """
    Subscribe users to a particular subscription group
    :param subscription_group_user_deltas: Maps subscription group names (POCKET_HITS_US_DAILY,
    POCKET_HITS_US_WEEKLY, POCKET_HITS_DE_DAILY) to UserDelta objects with users that should be subscribed to that group
    """
    logger = get_run_logger()

    subscription_group_name, user_deltas = subscription_group_user_deltas
    for chunk in chunks(user_deltas, SUBSCRIPTION_SET_LIMIT):
        logger.info(
            f"Subscribing {len(chunk)} users to {subscription_group_name}"
            f" with event_ids=[{[c.event_id for c in chunk]}]"
        )

        BrazeClient(logger=logger).subscribe_users(  # type: ignore
            models.SubscribeUsersInput(
                subscription_group_id=SUBSCRIPTION_GROUP_NAME_TO_ID[
                    subscription_group_name
                ],
                subscription_state="subscribed",
                external_id=[
                    u.external_id for u in chunk if u.external_id is not None
                ],  # Identified users
                email=[
                    u.email
                    for u in chunk
                    if u.external_id is None and is_valid_email(u.email)  # type: ignore
                ],  # Alias-only # type: ignore
            )
        )


@task(retries=3, retry_delay_seconds=30)
def track_users(user_deltas: List[UserDelta]):
    """
    Sends attributes and events to Braze based on UserDelta objects. Also creates new users who have an external_id.
    :param user_deltas List of user_deltas
    """
    logger = get_run_logger()

    for chunk in chunks(user_deltas, USER_TRACK_LIMIT):
        logger.info(
            f"Tracking {len(chunk)} users with event_ids=[{[c.event_id for c in chunk]}]"
        )

        # Note: The attributes and events below are bulk updates. They're not necessarily of equal length or in order.
        BrazeClient(logger=logger).track_users(
            models.TrackUsersInput(
                attributes=get_attributes_for_user_deltas(chunk),
                events=get_events_for_user_deltas(chunk),
            )
        )


def get_attributes_for_user_deltas(
    user_deltas: Sequence[UserDelta],
) -> List[models.UserAttributes]:
    """
    :param user_deltas:
    :return: Braze user attributes based the given user_deltas. len(returned list) <= len(user_deltas).
    """
    return [
        models.UserAttributes(
            external_id=user_delta.external_id,
            user_alias=models.UserAlias(EMAIL_ALIAS_LABEL, user_delta.email)
            if not user_delta.external_id
            else None,
            _update_existing_only=True,  # Do not create new profiles if one does not exist.
            email=user_delta.email,
            is_premium=user_delta.is_premium,
            time_zone=user_delta.time_zone,
            country=user_delta.country,
            pocket_locale=user_delta.pocket_locale,
            email_subscribe=user_delta.email_subscribe,
        )
        for user_delta in user_deltas
    ]


def get_events_for_user_deltas(
    user_deltas: Sequence[UserDelta],
) -> List[models.UserEvent]:
    """
    :param user_deltas:
    :return: Braze custom events based the given user_deltas. len(returned list) <= len(user_deltas).
    """
    return [
        models.UserEvent(
            external_id=user_delta.external_id,
            user_alias=models.UserAlias("email", user_delta.email)
            if not user_delta.external_id
            else None,
            name=user_delta.braze_event_name,
            properties=get_event_properties_for_user_delta(user_delta),
            time=format_date(user_delta.happened_at),
        )
        for user_delta in user_deltas
        if user_delta.braze_event_name
    ]


def get_event_properties_for_user_delta(
    user_delta: UserDelta,
) -> models.EventPropertiesType:
    """
    Format the Braze event properties for a custom event.
    Brace doc with accepted types: https://www.braze.com/docs/api/objects_filters/event_object/#event-properties-object
    Pocket events: https://docs.google.com/spreadsheets/d/1HIR33seaMDh55vQnNxDJsKXI6I7Afku9gxHMuY7CbrM/edit#gid=41936054
    :param user_delta:
    :return:
    """
    if user_delta.braze_event_name == "newsletter_signup":
        return {
            "newsletter": user_delta.newsletter_signup_event_newsletter,
            "frequency": user_delta.newsletter_signup_event_frequency,
        }  # type: ignore
    else:
        return {}


@flow(
    # task_runner=DaskTaskRunner(cluster_kwargs={"n_workers": 4, "threads_per_worker": 1})
)
async def update_braze(
    is_backfill: bool = False, max_operations_per_task_run: int = 100000
):
    sfc = MozSnowflakeConnector()

    last_offset = await get_last_offset(
        offset_key=OFFSET_KEY,
        snowflake_connector=sfc,
    )

    # Prevent us from accidentally emailing users from our dev environment by changing all domains to @example.com,  # noqa: E501
    # unless we are in the production environment. See SQL logic for EXTRACT_QUERY.
    # Be aware: Sending a large number of emails to fake (@example.com) accounts can harm our email reputation score.  # noqa: E501

    def get_table_name(is_backfill: bool = False) -> str:
        table_name = "ANALYTICS.DBT_STAGING.STG_BRAZE_USER_DELTAS"
        if is_backfill:
            table_name = f"BACKFILLS_{CS.deployment_type}.STG_BRAZE_USER_DELTAS"
        return table_name

    def get_dev_expressions() -> tuple[str, str]:
        email_expression = "EMAIL"
        limit = ""
        if not CS.is_production:
            email_expression = "CASE WHEN EMAIL IS NOT NULL THEN (split_part(EMAIL, '@',  0) || '@example.com') ELSE NULL END as EMAIL"  # noqa: E501
            limit = "LIMIT 1000"
        return email_expression, limit

    email_expression, limit = get_dev_expressions()

    user_deltas_dicts = await snowflake_query(
        query=EXTRACT_QUERY.format(
            table_name=get_table_name(is_backfill),
            email_expression=email_expression,
            limit=limit,
        ),
        cursor_type=DictCursor,  # type: ignore
        params={"loaded_at_start": last_offset[0][0]},
        snowflake_connector=sfc,
    )

    # Convert Snowflake dicts to @dataclass objects and create chunks.
    all_user_deltas = get_user_deltas_from_dicts(user_deltas_dicts)  # type: ignore
    all_user_deltas_split = split_in_chunks(
        all_user_deltas,
        chunk_size=max_operations_per_task_run,
    )

    # Get new offset key based on maximum loaded_at of rows.  # noqa: E501
    new_offset = get_last_loaded_at(
        all_user_deltas,
        wait_for=[  # type: ignore
            all_user_deltas_split,
        ],
    )

    # Exit flow if now rows to process.
    if not new_offset:
        return

    # Get the user deltas that contain an email and create chunks
    user_deltas_with_email = filter_user_deltas_with_email(all_user_deltas)
    user_deltas_with_email_split = split_in_chunks(
        user_deltas_with_email,
        chunk_size=max_operations_per_task_run,
    )

    # Create an email only profile,
    # or add an email alias to an existing user profile,
    # for our deltas that contain an email
    create_email_aliases_task = create_email_aliases.map(
        user_deltas_with_email_split,
    )

    # Identify ('merge') Pocket users with their email alias any time we have a delta with an email address  # noqa: E501
    # This deletes their old email alias.
    identify_users_task = identify_users.map(
        user_deltas_with_email_split,
        wait_for=[create_email_aliases_task],  # type: ignore
    )

    # Subscribing users needs to happen after Pocket users and alias-only users have been created.  # noqa: E501
    newsletter_subscription_groups = group_user_deltas_by_newsletter_subscription_name(
        all_user_deltas
    )
    newsletter_subscription_groups_split = split_dict_of_lists_in_chunks(
        newsletter_subscription_groups,
        chunk_size=max_operations_per_task_run,
    )

    subscribe_users_task = subscribe_users.map(
        newsletter_subscription_groups_split,
        wait_for=[identify_users_task],  # type: ignore
    )

    # Apply attributes, events, and payments.
    # Note: Both external_id and email aliases cannot be set simultaneously for the same user in the same call  # noqa: E501
    track_users_task = track_users.map(
        all_user_deltas_split,
        wait_for=[subscribe_users_task],  # type: ignore
    )

    # Deleting user profiles needs to happen after all of our other calls,
    # because it is possible the latter will create non-existing profiles.
    delete_trigger_users = filter_user_deltas_by_trigger(
        all_user_deltas, trigger="account_delete"
    )
    delete_trigger_users_split = split_in_chunks(
        delete_trigger_users,
        chunk_size=max_operations_per_task_run,
    )

    delete_users_results = delete_user_profiles.map(
        delete_trigger_users_split,
        wait_for=[track_users_task],  # type: ignore
    )

    await upsert_new_offset(
        offset_key=OFFSET_KEY,
        new_offset=new_offset,
        snowflake_connector=sfc,
        **{
            "wait_for": [
                create_email_aliases_task,
                identify_users_task,
                subscribe_users_task,
                track_users_task,
                delete_users_results,
            ]
        },
    )


FLOW_SPEC = FlowSpec(
    flow=update_braze,
    docker_env="base",
    deployments=[
        FlowDeployment(name="update-braze"),
    ],
)

if __name__ == "__main__":
    import asyncio
    asyncio.run(update_braze())  # type: ignore
