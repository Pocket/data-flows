import json
from pathlib import Path
from typing import List, Dict
import datetime

import pandas as pd
from prefect import Flow, task

from api_clients.braze import BrazeClient, UserTracking, UserAttributes, format_date, REQUEST_ATTRIBUTE_LIMIT, UserEvent, UserAlias, Purchase, format_date
from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType

FLOW_NAME = Path(__file__).stem

# This query does some modeling that we'll eventually do in Dbt.
_EXTRACT_ACCOUNT_SIGNUP_QUERY = """
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
_EXTRACT_PREMIUM_SUBSCRIPTION_QUERY = """
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
        'tstamp_start': '2022-02-26 00:00:00',
        'tstamp_end': '2022-02-27 00:05:00',
    }


@task
def create_user_profiles(account_signups: pd.DataFrame):
    """
    Creates Braze user profiles

    Use cases:
    - If a user signs up for Pocket, they should get a Braze user profile, such that they can receive Pocket Hits if
    they’re eligible.

    :return:
    """
    _mask_email_domain(account_signups)

    for account_signup_chunk in _chunks(account_signups):
        attributes = [
            UserAttributes(
                external_id=account_signup['HASHED_USER_ID'],
                is_premium=True,
                date_of_first_session=format_date(account_signup['CREATED_AT']),
                time_zone=account_signup['TIMEZONE'],
                country=account_signup['COUNTRY'],
                home_city=account_signup['CITY']
            ) for account_signup in account_signup_chunk
        ]

        events = [
            UserEvent(
                #TODO: Map the snowplow event api id to a Braze App ID for targeting in braze based on platform.
                app_id=account_signup['BRAZE_API_ID'],
                external_id=account_signup['HASHED_USER_ID'],
                name="account_signup",
                time=format_date(account_signup['CREATED_AT']),
                #properties=,
                # user_alias=UserAlias(
                #     external_id=account_signup['HASHED_USER_ID'],
                #     alias_label="email",
                #     alias_name=account_signup['EMAIL']
                # )
            ) for account_signup in account_signup_chunk
        ]

        BrazeClient().track_users(user_tracking=UserTracking(
            attributes=attributes,
            events=events
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
    for account_signup_chunk in _chunks(account_signups):
        attributes = [
            UserAttributes(
                external_id=account_signup['HASHED_USER_ID'],
                is_premium=True,
            ) for account_signup in account_signup_chunk
        ]

        purchases = [
            Purchase(
                #TODO: Map the snowplow event api id to a Braze App ID for targeting in braze based on platform.
                app_id=premium_created['BRAZE_API_ID'],
                external_id=premium_created['HASHED_USER_ID'],
                product_id="pocket_premium", #TODO: This should be the Pocket Product ID and then we need to set up these maps in Braze
                price=premium_created['AMOUNT'],
                currency=premium_created['CURRENCY'],
                time=format_date(premium_created['CREATED_AT']),
                #properties=,
                # user_alias=UserAlias(
                #     external_id=account_signup['HASHED_USER_ID'],
                #     alias_label="email",
                #     alias_name=account_signup['EMAIL']
                # )
            ) for premium_created in account_signup_chunk
        ]

        response = BrazeClient().track_users(user_tracking=UserTracking(
            attributes=attributes,
            purchases=purchases,
        ))

        print(response)


def _chunks(index, n=REQUEST_ATTRIBUTE_LIMIT):
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
        row['EMAIL'] = _replace_email_domain(row['EMAIL'], new_domain='@example.com')


with Flow(FLOW_NAME) as flow:
    # account_signup_snowplow_events = PocketSnowflakeQuery()(
    #     query=_EXTRACT_ACCOUNT_SIGNUP_QUERY,
    #     data=get_extract_query_parameters(trigger='account_signup'),
    #     output_type=OutputType.DICT,
    # )
    #
    # create_user_profiles(account_signup_snowplow_events)

    premium_subscription_created_events = PocketSnowflakeQuery()(
        query=_EXTRACT_PREMIUM_SUBSCRIPTION_QUERY,
        data=get_extract_query_parameters(trigger='payment_subscription_created'),
        output_type=OutputType.DICT,
    )

    make_user_premium(premium_subscription_created_events)


if __name__ == "__main__":
    flow.run()
