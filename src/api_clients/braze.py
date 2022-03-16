import datetime
import json
import logging
import requests
from dataclasses import dataclass, field
from typing import Dict, List, Union

from utils import config
from utils.dataclasses import Missing, DataClassJSONEncoder


"""
Maximum number of attributes that can be updated in a single request.
"""
USER_TRACK_LIMIT = 75
USER_DELETE_LIMIT = 50
NEW_USER_ALIAS_LIMIT = 50

"""
Email alias name
"""
EMAIL_ALIAS_LABEL = 'email'

BRAZE_APP_ID_TO_POCKET = {
    '5511': '949dbb2a-e619-42dc-8d5c-d6d4c9d2380b',  # iOS
    '5512': '949dbb2a-e619-42dc-8d5c-d6d4c9d2380b',  # iOS
    '5513': '949dbb2a-e619-42dc-8d5c-d6d4c9d2380b',  # Android
    '5514': '949dbb2a-e619-42dc-8d5c-d6d4c9d2380b',  # Android
    '': '949dbb2a-e619-42dc-8d5c-d6d4c9d2380b',  # Web
}


@dataclass
class UserAlias:
    """
    Key of the alias. For example 'amplitude_id'.
    https://www.braze.com/docs/user_guide/data_and_analytics/user_data_collection/user_profile_lifecycle/#user-aliases
    """
    alias_label: str
    """Value of the alias. This in combination with the alias_label must be unique for the user."""
    """If the value is the same as another user, Braze will merge the 2 profiles."""
    alias_name: str


@dataclass
class UserAliasExternalIdAssociation(UserAlias):
    external_id: Union[str, Missing] = Missing()  # Masked Pocket user id,


@dataclass
class _UserIdentifier:
    # One of `external_id` or `user_alias` or `braze_id` is required
    external_id: Union[str, Missing] = Missing()  # Masked Pocket user id,
    user_alias: Union[str, UserAlias] = Missing()
    braze_id: Union[str, Missing] = Missing()  # Braze User Identifier,

    def __post_init__(self):
        ids = [self.external_id, self.user_alias, self.braze_id]
        # Validate that exactly one id field is provided.
        assert list(type(i) is not Missing for i in ids).count(True) == 1, f'One user ID required in {ids}'


@dataclass
class UserAttributes(_UserIdentifier):
    """
    @see https://www.braze.com/docs/api/objects_filters/user_attributes_object/
    """

    # Setting this flag to true will put the API in "Update Only" mode.
    # When using a "user_alias", "Update Only" defaults to true.
    _update_existing_only: Union[bool, Missing] = Missing()

    # Braze User Profile Fields
    email: Union[str, Missing] = Missing()
    first_name: Union[str, Missing] = Missing()
    last_name: Union[str, Missing] = Missing()
    #(date at which the user first used the app) String in ISO 8601 format or in yyyy-MM-dd'T'HH:mm:ss:SSSZ format.
    #Also the pocket signed up at
    date_of_first_session: Union[str, Missing] = Missing()
    email_subscribe: Union[str, Missing] = Missing()

    #(string) We require that country codes be passed to Braze in the ISO-3166-1 alpha-2 standard .
    country: Union[str, Missing] = Missing()

    #(string) we require that language be passed to Braze in the ISO-639-1 standard .
    language: Union[str, Missing] = Missing()

    # (string) Of time zone name from IANA Time Zone Database (e.g., “America/New_York” or “Eastern Time (US & Canada)”). Only valid time zone values will be set.
    time_zone: Union[str, Missing] = Missing()

    # City of the user
    home_city: Union[str, Missing] = Missing()

    # Custom Attributes
    is_premium: Union[bool, Missing] = Missing()

    # # Adding a new value to an array custom attribute
    # my_array_custom_attribute: { "add": ["Value3"] }
    # # Removing a value from an array custom attribute
    # my_array_custom_attribute: { "remove": [ "Value1" ]}


"""
@see https://www.braze.com/docs/api/objects_filters/event_object/#event-properties-object
"""
EventPropertyValueType = Union[
    int,
    float,
    bool,
    str,  # length <= 255 string. Will be interpreted as a date if formatted as ISO-8601 or yyyy-MM-dd'T'HH:mm:ss:SSSZ
    List,  # Arrays cannot include datetimes.
]


@dataclass
class _UserEventRequiredFields:
    name: str  # required, the name of the event
    time: str  # required, datetime as string in ISO 8601 or in `yyyy-MM-dd'T'HH:mm:ss:SSSZ` format


@dataclass
class UserEvent(_UserIdentifier, _UserEventRequiredFields):
    """
    @see https://www.braze.com/docs/api/objects_filters/event_object/
    """

    app_id: Union[str, Missing] = Missing()  # See https://www.braze.com/docs/api/api_key/#the-app-identifier-api-key

    """
    Event properties key-value. Key string length <= 255 characters, with no leading $ sign.
    """
    properties: Dict[str, EventPropertyValueType] = field(default_factory=dict)

    # Setting this flag to true will put the API in "Update Only" mode.
    # When using a "user_alias", "Update Only" defaults to true.
    _update_existing_only: Union[bool, Missing] = Missing()


@dataclass
class _PurchaseRequiredFields:
    app_id: str  # Required, see https://www.braze.com/docs/api/api_key/#the-app-identifier-api-key
    time: str  # required, datetime as string in ISO 8601, Time of purchase

    product_id: str  # identifier for the purchase, e.g. Product Name or Product Category
    currency: str  # ISO 4217 Alphabetic Currency Code,
    # Revenue from a purchase object is calculated as the product of quantity and price.
    price: float  # value in the base currency unit (e.g. Dollars for USD, Yen for JPY),


@dataclass
class Purchase(_UserIdentifier, _PurchaseRequiredFields):
    """
    @see https://www.braze.com/docs/api/objects_filters/event_object/
    """

    # the quantity purchased (defaults to 1, must be <= 100 -- currently, Braze treats a quantity _X_ as _X_ separate
    # purchases with quantity 1),
    quantity: Union[int, Missing] = Missing()

    properties: Dict[str, EventPropertyValueType] = field(default_factory=dict)  # Key string length <= 255 characters, with no leading $ sign.

    # Setting this flag to true will put the API in "Update Only" mode.
    # When using a "user_alias", "Update Only" defaults to true.
    _update_existing_only: Union[bool, Missing] = Missing()


@dataclass
class TrackUsersInput:
    attributes: Union[List[UserAttributes], Missing] = Missing()
    events: Union[List[UserEvent], Missing] = Missing()
    purchases: Union[List[Purchase], Missing] = Missing()


@dataclass
class UserDeleteInput:
    external_ids: List[str]


@dataclass
class CreateUserAliasInput:
    user_aliases: List[UserAliasExternalIdAssociation]


def format_date(dt: datetime.datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


class BrazeClient:

    def __init__(
            self,
            session: requests.Session = None,
            api_key: str = config.BRAZE_API_KEY,
            rest_endpoint: str = config.BRAZE_REST_ENDPOINT,
            logger: logging.Logger = logging.getLogger(),
    ):
        """
        :param session: (optional) HTTP session. If None, a new session will be created.
        :param api_key: Braze API key. Loaded from environment variable 'BRAZE_API_KEY' by default.
        :param rest_endpoint: Braze REST endpoint. Loaded from environment variable 'BRAZE_REST_ENDPOINT' by default.
        :param logger: Set to `prefect.context.get("logger")` to use the Prefect logger.
        """
        self._session = session if session is not None else requests.Session()
        self._api_key = api_key
        self._rest_endpoint = rest_endpoint
        self._logger = logger

    def create_new_user_aliases(self, user_aliases: CreateUserAliasInput):
        """
        Batch create aliases for one or more users.
        @see https://documenter.getpostman.com/view/4689407/SVYrsdsG?version=latest#22e91d00-d178-4b4f-a3df-0073ecfcc992
        :return:
        """
        return self._post_request('/users/alias/new', user_aliases)

    def track_users(self, user_tracking: TrackUsersInput):
        """
        Batch update attributes and events for one or more users.
        @see https://documenter.getpostman.com/view/4689407/SVYrsdsG?version=latest#4cf57ea9-9b37-4e99-a02e-4373c9a4ee59
        :return:
        """
        return self._post_request('/users/track?=', user_tracking)

    def delete_users(self, users_to_delete: UserDeleteInput):
        """
        Batch delete users
        @see https://documenter.getpostman.com/view/4689407/SVYrsdsG?version=latest#22e91d00-d178-4b4f-a3df-0073ecfcc992
        :return:
        """
        return self._post_request('/users/delete', users_to_delete)

    def _post_request(self, path, braze_data):
        #TODO: Look at braze bulk header https://www.braze.com/docs/api/endpoints/user_data/post_user_track/#making-bulk-updates
        response = self._session.post(
            self._rest_endpoint + path,
            data=json.dumps(braze_data, cls=DataClassJSONEncoder),
            headers={
                'Authorization': f'Bearer {self._api_key}',
                'Content-Type': 'application/json',
            },
        )
        self._logger.info(f"Braze {path} responded with {response.status_code}: {response.text}")
        return response
