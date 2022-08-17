import json
import re
import uuid
from typing import Dict, List

import pytest

from common_tasks.corpus_candidate_set import validate_corpus_items, create_corpus_candidate_set_record


@pytest.fixture
def corpus_items_100():
    return [{'ID': str(uuid.uuid4()), 'TOPIC': str(i)} for i in range(100)]


@pytest.fixture
def corpus_candidate_set_id():
    return str(uuid.uuid4())


def test_validate_corpus_items_does_not_raise_an_exception(corpus_items_100):
    validate_corpus_items.run([{'ID': 'c9bf9e57-1685-4c89-bafb-ff5af830be8a', 'TOPIC': 'BUSINESS'}])


def test_validate_corpus_items_returns_corpus_items(corpus_items_100):
    assert corpus_items_100 == validate_corpus_items.run(corpus_items_100)


@pytest.mark.parametrize(
    "corpus_items",
    [
        ([]),                                                                     # List must be non-empty
        ([{'TOPIC': 'BUSINESS'}]),                                                # Must contain 'ID' key
        ([{'id': 'c9bf9e57-1685-4c89-bafb-ff5af830be8a'}]),                       # Must contain 'TOPIC'
        ([{'id': 'c9bf9e57-1685-4c89-bafb-ff5af830be8a', 'TOPIC': 'BUSINESS'}]),  # 'ID' key is case-sensitive
        ([{'ID': 'c9bf9e57-1685-4c89-bafb-ff5af830be8a', 'Topic': 'BUSINESS'}]),  # 'TOPIC' key is case-sensitive
        ([{'ID': 1234, 'TOPIC': 'BUSINESS'}]),                                      # ID must be a string
        ([{'ID': '', 'TOPIC': 'BUSINESS'}]),                                      # 'ID' key must be non-empty
        ([{'ID': 'c9bf9e57-1685-4c89-bafb-ff5af830be8a', 'TOPIC': ''}]),          # 'TOPIC' must be non-empty
        ([{'ID': 'c9bf9e57-1685-4c89-bafb-ff5af830be8a', 'TOPIC': 'BUSINESS', 'FOO': 'BAR'}]),  # No other keys expected
    ],
)
def test_validate_corpus_items_assertion_failed(corpus_items: List[Dict]):
    with pytest.raises(AssertionError):
        validate_corpus_items.run(corpus_items)


def test_create_corpus_candidate_set_record(corpus_candidate_set_id, corpus_items_100):
    feature_values = create_corpus_candidate_set_record.run(
        id=corpus_candidate_set_id,
        corpus_items=corpus_items_100,
    )

    assert len(feature_values) == 3

    assert feature_values[0].feature_name == 'id'
    assert feature_values[0].value_as_string == corpus_candidate_set_id

    assert feature_values[1].feature_name == 'unloaded_at'
    assert re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', feature_values[1].value_as_string)  # ISO time format

    assert feature_values[2].feature_name == 'corpus_items'
    assert json.loads(feature_values[2].value_as_string) == corpus_items_100
