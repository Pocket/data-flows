from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union


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

    def __post_init__(self):
        assert self.alias_label and self.alias_name is not None


@dataclass
class UserAliasExternalIdAssociation(UserAlias):
    external_id: Optional[str] = None  # Masked Pocket user id,


@dataclass
class UserAliasIdentifier:
    external_id: str
    user_alias: UserAlias


@dataclass
class _UserIdentifier:
    # One of `external_id` or `user_alias` or `braze_id` is required
    external_id: Optional[str] = None  # Masked Pocket user id,
    user_alias: Union[str, UserAlias] = None
    braze_id: Optional[str] = None  # Braze User Identifier,

    def __post_init__(self):
        ids = [self.external_id, self.user_alias, self.braze_id]
        # Validate that exactly one id field is provided.
        assert list(v is not None for v in ids).count(True) == 1, f'Exactly one user ID required in {ids}'


@dataclass
class UserAttributes(_UserIdentifier):
    """
    @see https://www.braze.com/docs/api/objects_filters/user_attributes_object/
    """

    # Setting this flag to true will put the API in "Update Only" mode.
    # When using a "user_alias", "Update Only" defaults to true.
    _update_existing_only: Optional[bool] = None

    # Braze User Profile Fields
    email: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    #(date at which the user first used the app) String in ISO 8601 format or in yyyy-MM-dd'T'HH:mm:ss:SSSZ format.
    #Also the pocket signed up at
    date_of_first_session: Optional[str] = None
    email_subscribe: Optional[str] = None

    # Braze requires that country codes be passed to Braze in the ISO-3166-1 alpha-2 standard.
    country: Optional[str] = None

    # Braze requires that language be passed to Braze in the ISO-639-1 standard.
    language: Optional[str] = None

    # Time zone name from IANA Time Zone Database (e.g., “America/New_York”). Only valid time zone values will be set.
    time_zone: Optional[str] = None

    # Pocket's custom Attributes
    is_premium: Optional[bool] = None


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


EventPropertiesType = Dict[str, EventPropertyValueType]


@dataclass
class _UserEventRequiredFields:
    name: str  # required, the name of the event
    time: str  # required, datetime as string in ISO 8601 or in `yyyy-MM-dd'T'HH:mm:ss:SSSZ` format


@dataclass
class UserEvent(_UserIdentifier, _UserEventRequiredFields):
    """
    @see https://www.braze.com/docs/api/objects_filters/event_object/
    """

    app_id: Optional[str] = None  # See https://www.braze.com/docs/api/api_key/#the-app-identifier-api-key

    """
    Event properties key-value. Key string length <= 255 characters, with no leading $ sign.
    """
    properties: EventPropertiesType = field(default_factory=dict)

    # Setting this flag to true will put the API in "Update Only" mode.
    # When using a "user_alias", "Update Only" defaults to true.
    _update_existing_only: Optional[bool] = None


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
    quantity: Optional[int] = None

    properties: Dict[str, EventPropertyValueType] = field(default_factory=dict)  # Key string length <= 255 characters, with no leading $ sign.

    # Setting this flag to true will put the API in "Update Only" mode.
    # When using a "user_alias", "Update Only" defaults to true.
    _update_existing_only: Optional[bool] = None


@dataclass
class TrackUsersInput:
    attributes: Optional[List[UserAttributes]] = None
    events: Optional[List[UserEvent]] = None
    purchases: Optional[List[Purchase]] = None


@dataclass
class UserDeleteInput:
    external_ids: List[str]


@dataclass
class SubscribeUsersInput:
    subscription_group_id: str
    subscription_state: str
    external_id: Optional[List[str]] = None
    email: Optional[List[str]] = None


@dataclass
class CreateUserAliasInput:
    user_aliases: List[UserAliasExternalIdAssociation]


@dataclass
class IdentifyUsersInput:
    aliases_to_identify: List[UserAliasIdentifier]


