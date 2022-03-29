from utils.config import ENVIRONMENT, ENV_PROD

"""
EMAIL_ALIAS_LABEL defines the label we use for email aliases.
Currently we don't have any other alias labels besides email.
"""
EMAIL_ALIAS_LABEL = 'email'

"""
The dictionary below maps subscription group names (internally defined at Pocket) to Braze subscription ids.
This is defined here because we don't have a separate dev/prod Dbt environment, so the Dbt model abstracts away from
this by using the same name (e.g. POCKET_HITS_US_DAILY) for the daily US Pocket Hits newsletter. 

Definition of names: https://github.com/Pocket/dbt-snowflake/blob/master/models/staging/braze/stg_braze_user_deltas.sql
List of subscription group ids: https://dashboard-05.braze.com/users/subscription_groups/subscription_groups
"""
SUBSCRIPTION_GROUP_NAME_TO_ID = {
    # Production subscription groups
    'POCKET_HITS_US_DAILY': 'fbc3506c-26a5-4f35-b8f0-ed382da4f2db',
    'POCKET_HITS_US_WEEKLY': '0b88d11c-a856-4478-8f99-54e0178bcd70',
    'POCKET_HITS_DE_DAILY': 'e74c6d80-9cf7-4377-91c0-6bcce384ba77',
} if ENVIRONMENT == ENV_PROD else {
    # Development subscription groups
    'POCKET_HITS_US_DAILY': '0e63c4fa-30fb-445f-bf4f-583e5dd10efb',
    'POCKET_HITS_US_WEEKLY': '99fbeac9-2fa3-496d-ab94-4e869d42e52c',
    'POCKET_HITS_DE_DAILY': '69de5416-2bc1-44e4-8b5e-1e03bb86dcf5',
}
