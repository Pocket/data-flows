from prefect.client import Client
from prefect.utilities.graphql import (
    EnumValue,
    GraphQLResult,
    compress,
    parse_graphql,
    with_args,
    format_graphql_request_error,
)


def get_flow_group_id_by_flow_id(flow_id: str) -> str:
    # Query for flow group id
    res = Client().graphql(
        {
            "query": {
                with_args("flow_by_pk", {"id": flow_id}): {"flow_group_id": ...}
            }
        }
    )

    return res.get("data").flow_by_pk.flow_group_id
