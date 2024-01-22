import logging
from unittest.mock import MagicMock

import pendulum
import pytest
import requests
import requests_mock
from braze.update_flow import (
    SUBSCRIPTION_GROUP_NAME_TO_ID,
    BrazeClient,
    filter_user_deltas_by_trigger,
    filter_user_deltas_with_email,
    get_attributes_for_user_deltas,
    get_events_for_user_deltas,
    get_user_deltas_from_dicts,
    group_user_deltas_by_newsletter_subscription_name,
    is_valid_email,
    models,
    split_dict_of_lists_in_chunks,
    split_in_chunks,
    update_braze,
)
from prefect import task
from shared.iteration_utils import chunks

TEST_DATETIME = pendulum.from_format("2024-01-11 12:35", "YYYY-MM-DD HH:mm")
TEST_CHUNK_SIZE = 10


def create_fake_data(with_variation: bool = False, with_results: bool = True):
    test_data = [
        {
            "BRAZE_EVENT_NAME": "account_sign_up",
            "COUNTRY": "FR",
            "EMAIL": "test@example.com",
            "EVENT_ID": "c0e6a6de-b755-4548-bdd5-6eb7b048afdb",
            "EXTERNAL_ID": "1d3g9d77pe9d1A97a6TTh28T01A0p723b13if8Ve15m674E5575a1xaMTf6ZuaN9",
            "HAPPENED_AT": TEST_DATETIME.add(minutes=i),
            "HAS_EMAIL": True,
            "LOADED_AT": TEST_DATETIME.add(minutes=i + 1),
            "POCKET_LOCALE": "fr-FR",
            "TIME_ZONE": "Europe/Paris",
            "USER_EVENT_TRIGGER": "payment_subscription_renewed",
            "IS_PREMIUM": None,
            "SUBSCRIBE_TO_NEWSLETTER_SUBSCRIPTION_GROUP_NAME": None,
            "EMAIL_SUBSCRIBE": None,
            "NEWSLETTER_SIGNUP_EVENT_NEWSLETTER": None,
            "NEWSLETTER_SIGNUP_EVENT_FREQUENCY": None,
        }
        for i in range(0, 100)
    ]
    # adjust fake data is with_variation
    if with_variation:
        for i in test_data[5:25]:
            i[
                "SUBSCRIBE_TO_NEWSLETTER_SUBSCRIPTION_GROUP_NAME"
            ] = "POCKET_HITS_US_DAILY"
            i["BRAZE_EVENT_NAME"] = "newsletter_signup"
        for i in test_data[5:15]:
            i["HAS_EMAIL"] = False
            i["USER_EVENT_TRIGGER"] = "account_delete"
            i["EXTERNAL_ID"] = None
            i["EMAIL"] = ""
    if not with_results:
        test_data = []
    return test_data


@pytest.mark.asyncio
@pytest.mark.parametrize("with_backfill_results", [True, False])
async def test_update_flow(with_backfill_results, monkeypatch):
    fake_data = create_fake_data(with_backfill_results, with_backfill_results)
    fake_offset = [(TEST_DATETIME,)]
    fake_response = []

    result_mapping = [fake_offset, fake_data, fake_response]

    # state for tracking fake task calls
    mock_state = {"sf_call_count": 0, "offset_call_count": 0}

    @task()
    async def fake_offset_task(*args, **kwargs):
        mock_state["offset_call_count"] += 1
        results = result_mapping[0]
        result_mapping.pop(0)
        return results

    @task()
    async def fake_sf_task(*args, **kwargs):
        mock_state["sf_call_count"] += 1
        return fake_data

    monkeypatch.setattr("braze.update_flow.snowflake_query", fake_sf_task)
    monkeypatch.setattr("braze.update_flow.get_last_offset", fake_offset_task)
    monkeypatch.setattr("braze.update_flow.upsert_new_offset", fake_offset_task)
    mock_client = MagicMock()
    monkeypatch.setattr("braze.update_flow.BrazeClient", mock_client)

    mock_client_count = 8
    mock_offset_count = 2
    if not with_backfill_results:
        mock_client_count = 0
        mock_offset_count = 1

    await update_braze(with_backfill_results)
    assert mock_client.call_count == mock_client_count
    assert mock_state["sf_call_count"] == 1
    assert mock_state["offset_call_count"] == mock_offset_count


@pytest.mark.parametrize("with_variation", [True, False])
def test_braze_utils(with_variation, monkeypatch):
    monkeypatch.setattr("braze.update_flow.get_run_logger", logging.getLogger)
    adapter = requests_mock.Adapter()
    session = requests.Session()
    session.mount("mock://", adapter)
    adapter.register_uri(
        requests_mock.ANY,
        requests_mock.ANY,
        json={"message": "successful"},
        status_code=201,
    )

    bc = BrazeClient(session=session)

    fake_data = create_fake_data(with_variation)

    variation_assert_mapping = {
        True: [
            (
                100,
                TEST_CHUNK_SIZE,
            ),
            (
                90,
                TEST_CHUNK_SIZE - 1,
            ),
            (
                1,
                {
                    "alias_label": "test",
                    "alias_name": "test@example.com",
                    "external_id": "1d3g9d77pe9d1A97a6TTh28T01A0p723b13if8Ve15m674E5575a1xaMTf6ZuaN9",
                },
            ),
            (
                1,
                {
                    "external_id": "1d3g9d77pe9d1A97a6TTh28T01A0p723b13if8Ve15m674E5575a1xaMTf6ZuaN9",
                    "user_alias": {
                        "alias_label": "test",
                        "alias_name": "test@example.com",
                    },
                },
            ),
            (
                {
                    "subscription_group_id": "0e63c4fa-30fb-445f-bf4f-583e5dd10efb",
                    "subscription_state": "subscribed",
                },
            ),
            (
                10,
                {
                    "external_id": "1d3g9d77pe9d1A97a6TTh28T01A0p723b13if8Ve15m674E5575a1xaMTf6ZuaN9",
                    "_update_existing_only": True,
                    "email": "test@example.com",
                    "country": "FR",
                    "time_zone": "Europe/Paris",
                    "pocket_locale": "fr-FR",
                },
            ),
            ({"external_ids": []},),
        ],
        False: [
            (
                100,
                TEST_CHUNK_SIZE,
            ),
            (
                100,
                TEST_CHUNK_SIZE,
            ),
            (
                1,
                {
                    "alias_label": "test",
                    "alias_name": "test@example.com",
                    "external_id": "1d3g9d77pe9d1A97a6TTh28T01A0p723b13if8Ve15m674E5575a1xaMTf6ZuaN9",
                },
            ),
            (
                1,
                {
                    "external_id": "1d3g9d77pe9d1A97a6TTh28T01A0p723b13if8Ve15m674E5575a1xaMTf6ZuaN9",
                    "user_alias": {
                        "alias_label": "test",
                        "alias_name": "test@example.com",
                    },
                },
            ),
            (),
            (
                10,
                {
                    "external_id": "1d3g9d77pe9d1A97a6TTh28T01A0p723b13if8Ve15m674E5575a1xaMTf6ZuaN9",
                    "_update_existing_only": True,
                    "email": "test@example.com",
                    "country": "FR",
                    "time_zone": "Europe/Paris",
                    "pocket_locale": "fr-FR",
                },
            ),
            (),
        ],
    }

    assertions = variation_assert_mapping[with_variation]

    # Test prefect task functions
    all_user_deltas = get_user_deltas_from_dicts.fn(fake_data)  # type: ignore
    all_user_deltas_split = split_in_chunks.fn(
        all_user_deltas,
        chunk_size=TEST_CHUNK_SIZE,
    )

    assert len(all_user_deltas) == assertions[0][0]
    assert len(all_user_deltas_split) == assertions[0][1]

    user_deltas_with_email = filter_user_deltas_with_email.fn(all_user_deltas)
    user_deltas_with_email_split = split_in_chunks.fn(
        user_deltas_with_email,
        chunk_size=TEST_CHUNK_SIZE,
    )

    assert len(user_deltas_with_email) == assertions[1][0]
    assert len(user_deltas_with_email_split) == assertions[1][1]

    for chunk in chunks(user_deltas_with_email_split[0], TEST_CHUNK_SIZE):
        bc.create_new_user_aliases(
            models.CreateUserAliasInput(
                user_aliases=[
                    models.UserAliasExternalIdAssociation(
                        external_id=user.external_id,
                        alias_label="test",
                        alias_name=user.email,
                    )
                    for user in chunk
                ]
            )
        )
        results = adapter.request_history[-1].json()
        assert len(results) == assertions[2][0]
        assert results["user_aliases"][0] == assertions[2][1]
        adapter.reset()
        bc.identify_users(
            models.IdentifyUsersInput(
                aliases_to_identify=[
                    models.UserAliasIdentifier(
                        external_id=user.external_id,
                        user_alias=models.UserAlias(
                            alias_label="test",
                            alias_name=user.email,
                        ),
                    )
                    for user in chunk
                ]
            )
        )
        results = adapter.request_history[-1].json()
        assert len(results) == assertions[3][0]
        assert results["aliases_to_identify"][0] == assertions[3][1]
        adapter.reset()

    # Subscribing users needs to happen after Pocket users and alias-only users have been created.  # noqa: E501
    newsletter_subscription_groups = (
        group_user_deltas_by_newsletter_subscription_name.fn(all_user_deltas)
    )
    newsletter_subscription_groups_split = split_dict_of_lists_in_chunks.fn(
        newsletter_subscription_groups,
        chunk_size=TEST_CHUNK_SIZE,
    )
    if newsletter_subscription_groups_split:
        subscription_group_name, user_deltas = newsletter_subscription_groups_split[0]
        for chunk in chunks(user_deltas, TEST_CHUNK_SIZE):
            bc.subscribe_users(  # type: ignore
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

            results = adapter.request_history[-1].json()
            assert results == assertions[4][0]
            adapter.reset()

    for chunk in chunks(all_user_deltas_split[0], TEST_CHUNK_SIZE):
        # Note: The attributes and events below are bulk updates. They're not necessarily of equal length or in order.
        bc.track_users(
            models.TrackUsersInput(
                attributes=get_attributes_for_user_deltas(chunk),
                events=get_events_for_user_deltas(chunk),
            )
        )

        results = adapter.request_history[-1].json()["attributes"]
        assert len(results) == assertions[5][0]
        assert results[0] == assertions[5][1]
        adapter.reset()

    # Deleting user profiles needs to happen after all of our other calls,
    # because it is possible the latter will create non-existing profiles.
    delete_trigger_users = filter_user_deltas_by_trigger.fn(
        all_user_deltas, trigger="account_delete"
    )
    delete_trigger_users_split = split_in_chunks.fn(
        delete_trigger_users,
        chunk_size=TEST_CHUNK_SIZE,
    )

    if delete_trigger_users:
        for chunk in chunks(delete_trigger_users_split[0], TEST_CHUNK_SIZE):
            bc.delete_users(
                models.UserDeleteInput(
                    external_ids=[u.external_id for u in chunk],
                )
            )

            results = adapter.request_history[-1].json()
            assert results == assertions[6][0]
