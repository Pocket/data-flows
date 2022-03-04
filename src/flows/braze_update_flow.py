from pathlib import Path
from typing import List, Dict

import pandas as pd
from prefect import Flow, task, context

from api_clients import braze
from api_clients.braze import BrazeClient
from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType

FLOW_NAME = Path(__file__).stem

# This query does some modeling that we'll eventually do in Dbt.
EXTRACT_ACCOUNT_SIGNUP_QUERY = """
    SELECT   
        e.GEO_COUNTRY as COUNTRY,
        e.GEO_CITY as CITY,
        e.GEO_TIMEZONE as TIMEZONE,
        e.CONTEXTS_COM_POCKET_ACCOUNT_1[0]['hashed_user_id']::STRING as HASHED_USER_ID,
        TO_TIMESTAMP(e.CONTEXTS_COM_POCKET_ACCOUNT_1[0]['created_at']) as CREATED_AT,
        e.CONTEXTS_COM_POCKET_ACCOUNT_1[0]['emails'][0]::STRING as EMAIL,
        e.CONTEXTS_COM_POCKET_ACCOUNT_1[0]['is_premium']::BOOLEAN as IS_PREMIUM,
        e.CONTEXTS_COM_POCKET_API_USER_1[0]['api_id']::STRING as POCKET_API_ID,
     CASE 
        WHEN POCKET_API_ID=5511 THEN '949dbb2a-e619-42dc-8d5c-d6d4c9d2380b'
        WHEN POCKET_API_ID=5512 THEN '949dbb2a-e619-42dc-8d5c-d6d4c9d2380b'
        WHEN POCKET_API_ID=5513 THEN '9660451f-20af-4330-b97f-da5159fb1d78'
        WHEN POCKET_API_ID=5514 THEN '9660451f-20af-4330-b97f-da5159fb1d78'
        ELSE '76e48d24-506c-4e7e-bec2-c0f262ebbcd5'
     END AS BRAZE_API_ID
    FROM SNOWPLOW.ATOMIC.EVENTS e
    WHERE event_name = 'object_update'
    AND UNSTRUCT_EVENT_COM_POCKET_OBJECT_UPDATE_1['trigger'] = %(trigger)s
    AND collector_tstamp BETWEEN %(tstamp_start)s AND %(tstamp_end)s
    """

# This query does some modeling that we'll eventually do in Dbt.
EXTRACT_USERS_TO_DELETE_QUERY = """
    SELECT   
        e.CONTEXTS_COM_POCKET_ACCOUNT_1[0]['hashed_user_id']::STRING as EXTERNAL_ID,
        TO_TIMESTAMP(e.CONTEXTS_COM_POCKET_ACCOUNT_1[0]['created_at']) as CREATED_AT
    FROM SNOWPLOW.ATOMIC.EVENTS e
    WHERE event_name = 'object_update'
    AND UNSTRUCT_EVENT_COM_POCKET_OBJECT_UPDATE_1['trigger'] = %(trigger)s
    AND collector_tstamp BETWEEN %(tstamp_start)s AND %(tstamp_end)s
    """

# This query does some modeling that we'll eventually do in Dbt.
EXTRACT_PREMIUM_SUBSCRIPTION_QUERY = """
    SELECT
        e.GEO_COUNTRY as COUNTRY,
        e.GEO_CITY as CITY,
        e.GEO_TIMEZONE as TIMEZONE,
        e.CONTEXTS_COM_POCKET_PAYMENT_SUBSCRIPTION_1[0]['currency']::STRING as CURRENCY,
        e.CONTEXTS_COM_POCKET_PAYMENT_SUBSCRIPTION_1[0]['amount']::FLOAT / 100 as AMOUNT,
        TO_TIMESTAMP(e.CONTEXTS_COM_POCKET_PAYMENT_SUBSCRIPTION_1[0]['created_at']) as CREATED_AT,
        e.CONTEXTS_COM_POCKET_USER_1[0]['hashed_user_id']::STRING as HASHED_USER_ID,
        e.CONTEXTS_COM_POCKET_API_USER_1[0]['api_id']::STRING as POCKET_API_ID,
     CASE 
        WHEN POCKET_API_ID=5511 THEN '949dbb2a-e619-42dc-8d5c-d6d4c9d2380b'
                WHEN POCKET_API_ID=5512 THEN '949dbb2a-e619-42dc-8d5c-d6d4c9d2380b'
        WHEN POCKET_API_ID=5513 THEN '9660451f-20af-4330-b97f-da5159fb1d78'
        WHEN POCKET_API_ID=5514 THEN '9660451f-20af-4330-b97f-da5159fb1d78'
        ELSE '76e48d24-506c-4e7e-bec2-c0f262ebbcd5'
     END AS BRAZE_API_ID
    FROM SNOWPLOW.ATOMIC.EVENTS e
    WHERE event_name = 'object_update'
    AND UNSTRUCT_EVENT_COM_POCKET_OBJECT_UPDATE_1['trigger'] = %(trigger)s
    AND collector_tstamp BETWEEN %(tstamp_start)s AND %(tstamp_end)s
    """


@task
def get_extract_query_parameters(trigger):
    """
    Return query parameters for _EXTRACT_QUERY

    Currently the values are constant. Eventually we'll pull these from the Prefect Key-Value store.
    :return:
    """
    return {
        'trigger': trigger,
        'tstamp_start': '2022-02-27 00:00:00',
        'tstamp_end': '2022-02-27 00:05:00',
    }


@task
def delete_user_profiles(users_to_delete: List[Dict]):
    """
    Deletes Braze user profiles

    Use cases:
    - If someone deletes their Pocket profile, their Braze user profile should also be deleted.
    """
    for account_signup_chunk in _chunks(users_to_delete, braze.USER_DELETE_LIMIT):
        BrazeClient(logger=context.get("logger")).delete_users(braze.UserDeleteInput(
            external_ids=[u['EXTERNAL_ID'] for u in account_signup_chunk]
        ))


@task
def create_user_profiles(account_signups: List[Dict]):
    """
    Creates Braze user profiles

    If a user signs up for Pocket:
    - They should get a Braze user profile, such that they can receive emails for which they're eligible.
    - An event should be sent to Braze such that a double opt-in email can be sent to German users.
    -

    :return:
    """
    _mask_email_domain(account_signups)

    for account_signup_chunk in _chunks(account_signups, braze.USER_TRACK_LIMIT):
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
                ) for account_signup in account_signup_chunk
            ],
            events=[
                braze.UserEvent(
                    app_id=account_signup['BRAZE_API_ID'],
                    external_id=account_signup['HASHED_USER_ID'],
                    name="account_signup",
                    time=braze.format_date(account_signup['CREATED_AT']),
                ) for account_signup in account_signup_chunk
            ]
        ))


@task
def make_user_premium(account_signups: pd.DataFrame):
    """
    Creates Braze user profiles

    Use cases:
    - If a user signs up for Pocket, they should get a Braze user profile, such that they can receive Pocket Hits if
    they’re eligible.

    :return:
    """
    for account_signup_chunk in _chunks(account_signups, braze.USER_TRACK_LIMIT):
        BrazeClient(logger=context.get("logger")).track_users(user_tracking=braze.TrackUsersInput(
            attributes=[
                braze.UserAttributes(
                    external_id=account_signup['HASHED_USER_ID'],
                    is_premium=True,
                ) for account_signup in account_signup_chunk
            ],
        ))


def _chunks(index, n):
    """Yield successive n-sized chunks from index."""
    for i in range(0, len(index), n):
        yield index[i: i + n]


def _replace_email_domain(email: str, new_domain) -> str:
    return email.split('@')[0] + new_domain


def _mask_email_domain(rows: List[Dict], email_column='EMAIL'):
    """
    For debugging purposes, change the domain of all email addresses to '@example.com'
    """
    for row in rows:
        row[email_column] = _replace_email_domain(row[email_column], new_domain='@example.com')


execute_snowflake_query = PocketSnowflakeQuery(output_type=OutputType.DICT)

with Flow(FLOW_NAME) as flow:
    account_signup_snowplow_events = execute_snowflake_query(
        query=EXTRACT_ACCOUNT_SIGNUP_QUERY,
        data=get_extract_query_parameters(trigger='account_signup'),
    )
    create_user_profiles(account_signup_snowplow_events)

    users_to_delete = execute_snowflake_query(
        query=EXTRACT_USERS_TO_DELETE_QUERY,
        data=get_extract_query_parameters(trigger='account_delete'),
    )
    delete_user_profiles(users_to_delete)

    premium_subscription_created_events = execute_snowflake_query(
        query=EXTRACT_PREMIUM_SUBSCRIPTION_QUERY,
        data=get_extract_query_parameters(trigger='payment_subscription_created'),
    )
    make_user_premium(premium_subscription_created_events)


if __name__ == "__main__":
    flow.run()
