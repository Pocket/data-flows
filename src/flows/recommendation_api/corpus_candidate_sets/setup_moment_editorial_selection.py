from prefect import Flow

from api_clients.pocket_snowflake_query import PocketSnowflakeQuery, OutputType
from common_tasks.corpus_candidate_set import (
    create_corpus_candidate_set_record,
    load_feature_record,
    feature_group,
    validate_corpus_items,
)
from utils import config
from utils.flow import get_flow_name, get_interval_schedule

FLOW_NAME = get_flow_name(__file__)

SETUP_MOMENT_EDITORIAL_CORPUS_CANDIDATE_SET_ID = '57d544d6-0758-4cd1-a7b4-86f454c8eae8'

"""
Data taken from: https://docs.google.com/spreadsheets/d/1gdl9695Ib6Kd0OUX28yD03wJzRAcHiKZkZqn9DVdI2I/edit#gid=1108537720

For steps how to dump this data see the flow readme (setup_moment_editorial_candidates_flow.md).
"""
CORPUS_IDS = [
    "d65e5540-6d1b-44d5-b3b7-40c3058d5f9e",
    "711c3163-2e66-40c5-874a-6d61d165160d",
    "0cceec34-8f2e-4801-b54c-c777b53a6b7a",
    "7070468d-fcd7-4aef-bb7b-1f162878e900",
    "d648a0af-0e75-46c6-b827-60918ef252c4",
    "0236d9fc-6d5a-4057-9c55-dbcfb902354f",
    "97160702-62a1-4501-9ced-cb3bee5449a1",
    "3fb298ac-90f2-428b-97d4-4156d8da921b",
    "c3be0f4b-13dd-4c9a-97bc-1d86ded08737",
    "cf478515-6235-4328-827a-f121e84d4fdb",
    "777b0427-0d75-49de-a540-500c52fa4ba6",
    "8f0f9172-73f7-43f5-abd8-b61f553f1273",
    "d30c2b93-3f4c-48c2-8318-32f403a5a920",
    "e339245c-7be8-4076-80cd-cc6d3a923f4b",
    "87d4f8c1-97a2-4609-b9a2-4be602833ee0",
    "1374dd3f-ccd8-4f7b-a83c-6a6b9e9fc3cc",
    "65ae6dd4-4308-4e06-b7e2-295bf4f884ef",
    "ba0330fb-b3e8-4eb0-aa02-8e9633f11f33",
    "0d1ffddf-194b-490c-af88-5ed4735c7fa0",
    "3ac13df2-0337-404e-a43c-a7299c998583",
    "78d4349f-0147-4ece-9058-e24f9b43542f",
    "a13b4c15-39f5-4c6a-bb2e-a88dd082785a",
    "8435a8d6-7622-474d-9cbd-9dde57cc01e1",
    "9cbde37c-dd53-437d-b2b6-9f4939dbbd6a",
    "8e34de82-bf6e-4edb-a5bf-7f16ad9d46a6",
    "414d2ef4-9623-4e1c-a623-96306a5237e1",
    "ddd89891-6ac1-40b5-88d6-f9862855ad14",
    "030ee0fc-0279-4a9a-b955-3bfa1581b4cd",
    "ad2a6668-6e08-4fc0-9018-1b287e76c5c1",
    "835921e7-9700-4f20-8e5f-4e5e3e3a2a9e",
    "f3ffd924-027c-4141-9bd2-5d3150336f2d",
    "f17ccc1d-ea4a-4daa-9234-f7a517fadf4c",
    "37b78dbe-5b30-4467-aa21-1e953274ca5a",
    "9faeea4f-ee3d-46ee-9e8b-3e2c0271eccb",
    "3fbd3b29-b18d-4e6f-827c-e39fbcb4d8cf",
    "bfe9f816-bd76-4522-b8f9-52263ae2c9ed",
    "49651a9e-2e19-4c25-b07a-b85d9c1462fc",
    "e1d1a192-73ae-4620-a6d2-7754aecc27a8",
    "b7013a6e-50e7-47a6-8862-cf073b8a37a0",
    "60cc9b9b-f926-431c-b8d4-97a0b5f58c35",
    "54f564b3-2eb4-424c-8087-8f624ff6d593",
    "1d8d2f22-7cef-4542-a3f8-827b4c01ce2f",
    "5284c1d4-7ced-4b19-8c69-8c48f38e5ce1",
    "d925b4aa-73bd-4f6d-bbb8-62a55cb75721",
    "b5ee84bb-2927-458f-8a6e-9461308714f5",
    "06baca35-2c22-4911-a70e-adb561dcc147",
    "e8dd547d-2db3-4e79-885a-f73bd246fb04",
    "e051efb0-6ec3-4752-96cc-f6b14eaeb201",
    "7b246ab7-5baf-4c34-90a7-eec02ba47321",
    "c4265085-6f10-4844-9395-7cea0ea83f6b",
]

"""
NOTE: I couldn't immediately find how to bind a list for parameterizing an IN clause. It's important to parameterize
      any value that can be user-controlled. In this case values are hard-coded, so it's safe to insert them directly.   
"""
EXPORT_CORPUS_ITEMS_SQL = f"""
SELECT
    APPROVED_CORPUS_ITEM_EXTERNAL_ID as ID,
    TOPIC,
    PUBLISHER
FROM "APPROVED_CORPUS_ITEMS"
WHERE APPROVED_CORPUS_ITEM_EXTERNAL_ID IN ({",".join(f"'{corpus_id}'" for corpus_id in CORPUS_IDS)});
"""


with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=30)) as flow:
    corpus_items = PocketSnowflakeQuery()(
        query=EXPORT_CORPUS_ITEMS_SQL,
        database=config.SNOWFLAKE_ANALYTICS_DATABASE,
        schema=config.SNOWFLAKE_ANALYTICS_DBT_SCHEMA,
        output_type=OutputType.DICT,
    )

    corpus_items = validate_corpus_items(corpus_items)

    feature_group_record = create_corpus_candidate_set_record(
        id=SETUP_MOMENT_EDITORIAL_CORPUS_CANDIDATE_SET_ID,
        corpus_items=corpus_items,
    )
    load_feature_record(feature_group_record, feature_group_name=feature_group)

if __name__ == "__main__":
    flow.run()
