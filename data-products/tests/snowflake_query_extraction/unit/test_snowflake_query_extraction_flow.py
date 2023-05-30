from snowflake_query_extraction.snowflake_query_extraction_flow import (
    extraction_query,
    snowflake_query_extraction,
    SfExtractionResult,
)
from prefect.testing.utilities import prefect_test_harness


def test_extraction_query():
    with prefect_test_harness():
        x = extraction_query.fn({})
        assert isinstance(x, SfExtractionResult)


def test_snowflake_query_extraction():
    with prefect_test_harness():
        snowflake_query_extraction({})
