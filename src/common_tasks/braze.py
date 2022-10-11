from typing import List, Dict

from prefect import task, context

from api_clients.braze import models
from api_clients.braze.client import BrazeClient
from utils import config

def _replace_email_domain(email: str, new_domain) -> str:
    """
    :param email: Email address
    :param new_domain:
    :return: Email address with the domain/host part replaced by new_domain
    """
    if '@' in email:
        return email.split('@')[0] + new_domain
    else:
        # Don't replace domain if there is none. If email = '', then we should keep it that way, to catch errors in dev.
        return email



@task()
def mask_email_domain_outside_production(rows: List[Dict], email_column='EMAIL'):
    """
    For debugging purposes, change the domain of all email addresses to '@example.com' in the local/dev environment.
    """
    if config.ENVIRONMENT != config.ENV_PROD:
        for row in rows:
            if row[email_column] is not None:
                row[email_column] = _replace_email_domain(row[email_column], new_domain='@example.com')

    return rows

@task()
def trigger_segment_export(segment_id: str):
    """
    Trigger an export of Braze Users
    """
    logger = context.get("logger")
    BrazeClient(logger=logger).users_by_segment(models.UsersBySegmentInput(
        segment_id=segment_id,
        fields_to_export=['external_id', 'user_aliases', 'braze_id', 'email']
    ))