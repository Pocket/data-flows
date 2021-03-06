import json
from datetime import datetime

import pytz
import prefect
from prefect import task
from prefect.triggers import all_successful

from prefect.backend import set_key_value, get_key_value

from utils.config import ENVIRONMENT


def format_key(flow_name: str, key_name: str) -> str:
    """
    Format a KV store key as ENVIRONMENT/flow_name/key_name
    :param flow_name: Name of the Prefect flow
    :param key_name: Last part of the key. Can be used to differentiate this key from other keys used in this flow.
    :return: KV store key
    """
    return '/'.join([ENVIRONMENT, flow_name, key_name])


def set_kv(key: str, value: str):
    return set_key_value(key, value)

def get_kv(key: str, default_value: str):
    try:
        return get_key_value(key)
    except ValueError as err:
        return default_value

@task
def get_last_executed_value(flow_name: str, default_if_absent='2000-01-01 00:00:00.000') -> datetime:
    """
    Query Prefect KV Store to get the execution date from previous Flow state

    Args:
        - flow_name: The name of the flow in Prefect Cloud to fetch metadata from
        - default_if_absent: The date to use as the last executed date if it is absent from the metadata, which will allow this flow to run the first time targeting a new Prefect Cloud env.

    Returns:
    'last_executed_date' from the json metadata that represents the most recent execution date before right now

    """
    default_state_params_json = json.dumps({'last_executed': default_if_absent})
    key = format_key(flow_name, 'json')
    state_params_json = get_kv(key, default_state_params_json)
    try:
        last_executed = json.loads(state_params_json).get('last_executed')
    except json.decoder.JSONDecodeError:
        # If the KV store has bad data
        last_executed = {'last_executed': default_if_absent, }

    logger = prefect.context.get("logger")
    logger.info(f"Loading data from Snowflake since {last_executed}")
    return datetime.strptime(last_executed, "%Y-%m-%d %H:%M:%S.%f")


@task(trigger=all_successful)
def update_last_executed_value(for_flow: str, default_if_absent='2000-01-01 00:00:00.000') -> None:
    """
     Does the following:
     - Increments the execution date by a variable amount, passed in via the named parameters to timedelta like days, hours, and seconds: Represents the next run data for the Flow
     - Updates the Prefect KV Store to set the 'last_executed' with the next execution date

     Args:
        - for_flow: The name of the flow in Prefect Cloud to write metadata to
        - default_if_absent: The date to use as the last executed date if it isn't specified. THIS RESETS THE FLOW to fetch every record from the table!!

     Returns:
     The next execution date
     """
    default_state_params_json = json.dumps({'last_executed': default_if_absent,})
    key = format_key(for_flow, 'json')
    state_params_json = get_kv(key, default_state_params_json)

    try:
        state_params_dict = json.loads(state_params_json)
    except json.decoder.JSONDecodeError:
        # If the KV store has bad data
        state_params_dict = {'last_executed': default_if_absent,}

    utcnow = datetime.utcnow()
    state_params_dict['last_executed'] = utcnow.strftime('%Y-%m-%d %H:%M:%S.%f')

    logger = prefect.context.get("logger")
    logger.info(f"Set last executed time to: {state_params_dict['last_executed']}")
    set_kv(key, json.dumps(state_params_dict))
