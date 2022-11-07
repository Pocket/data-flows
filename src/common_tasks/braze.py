from typing import List, Dict
from prefect import task
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
