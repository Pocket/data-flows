import copy
import json
import logging

import requests
from common.settings import NestedSettings, SecretSettings
from shared.api_clients.braze.models import (
    CreateUserAliasInput,
    IdentifyUsersInput,
    SubscribeUsersInput,
    TrackUsersInput,
    UserDeleteInput,
)
from shared.dataclass_utils import DataClassJSONEncoderWithoutNoneValues

# Maximum number of entities that can be updated in each request.
USER_TRACK_LIMIT = 75
USER_DELETE_LIMIT = 50
NEW_USER_ALIAS_LIMIT = 50
IDENTIFY_USER_ALIAS_LIMIT = 50
SUBSCRIPTION_SET_LIMIT = 50


# Create setting model for Braze API creds
class BrazeCredSettings(NestedSettings):
    api_key: str
    rest_endpoint: str


class BrazeSettings(SecretSettings):
    braze_credentials: BrazeCredSettings


class BrazeClient:
    """
    Client for Braze Rest API
    Docs: https://documenter.getpostman.com/view/4689407/SVYrsdsG?version=latest#903f23b8-dd85-4c82-bff8-9f651d916888
    """

    def __init__(
        self,
        session: requests.Session = None,  # type: ignore
        logger: logging.Logger = logging.getLogger(),
    ):
        """
        :param session: (optional) HTTP session. If None, a new session will be created.
        :param api_key: Braze API key. Loaded from environment variable 'BRAZE_API_KEY' by default.
        :param rest_endpoint: Braze REST endpoint. Loaded from environment variable 'BRAZE_REST_ENDPOINT' by default.
        :param logger: Set to `prefect.context.get("logger")` to use the Prefect logger.
        """  # noqa: E501
        self._session = session if session is not None else requests.Session()
        self._logger = logger
        braze_creds = BrazeSettings()  # type: ignore
        self._api_key = braze_creds.braze_credentials.api_key
        self._rest_endpoint = braze_creds.braze_credentials.rest_endpoint

    def create_new_user_aliases(self, user_aliases: CreateUserAliasInput):
        """
        Batch create aliases for one or more users.
        @see https://documenter.getpostman.com/view/4689407/SVYrsdsG?version=latest#22e91d00-d178-4b4f-a3df-0073ecfcc992
        :return:
        """
        return self._post_request("/users/alias/new", user_aliases)

    def identify_users(self, user_aliases: IdentifyUsersInput):
        """
        Batch identify ('merge') a user alias to an external_id.
        @see https://documenter.getpostman.com/view/4689407/SVYrsdsG?version=latest#22e91d00-d178-4b4f-a3df-0073ecfcc992
        :return:
        """
        return self._post_request("/users/identify", user_aliases)

    def track_users(self, user_tracking: TrackUsersInput):
        """
        Batch update attributes and events for one or more users.
        @see https://documenter.getpostman.com/view/4689407/SVYrsdsG?version=latest#4cf57ea9-9b37-4e99-a02e-4373c9a4ee59

        Internally, Braze applies attribute updates before it fires events,
        such that triggers based on these events can
        safely reference user attributes.
        :return:
        """
        return self._post_request("/users/track?=", user_tracking)

    def delete_users(self, users_to_delete: UserDeleteInput):
        """
        Batch delete users
        @see https://documenter.getpostman.com/view/4689407/SVYrsdsG?version=latest#22e91d00-d178-4b4f-a3df-0073ecfcc992
        :return:
        """
        return self._post_request("/users/delete", users_to_delete)

    def subscribe_users(self, subscribe_users_input: SubscribeUsersInput):
        """
        Batch subscribe users
        @see https://documenter.getpostman.com/view/4689407/SVYrsdsG?version=latest#22e91d00-d178-4b4f-a3df-0073ecfcc992
        :return:
        """
        # Remove empty lists for external_id and email because Braze will raise a 400 Bad Request they are empty.  # noqa: E501
        input_without_empty_lists = copy.copy(subscribe_users_input)
        if not input_without_empty_lists.external_id:
            input_without_empty_lists.external_id = None
        if not input_without_empty_lists.email:
            input_without_empty_lists.email = None

        return self._post_request("/subscription/status/set", input_without_empty_lists)

    def _post_request(self, path, braze_data):
        # TODO: Look at braze bulk header when backfilling data:
        # https://www.braze.com/docs/api/endpoints/user_data/post_user_track/#making-bulk-updates
        response = self._session.post(
            self._rest_endpoint + path,
            data=json.dumps(braze_data, cls=DataClassJSONEncoderWithoutNoneValues),
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
        )

        self._logger.info(
            f"Braze {path} responded with {response.status_code}: {response.text}"
        )

        response.raise_for_status()
        return response
