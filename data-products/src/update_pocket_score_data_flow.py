from typing import Union

import scipy.special
from common.databases.sqlalchemy_utils import MozSqlalchemyCredentials
from common.deployment.worker import FlowDeployment, FlowSpec
from prefect import flow, get_run_logger, task, unmapped
from prefect_sqlalchemy.database import sqlalchemy_execute, sqlalchemy_query

"""
Load and update the content quality score in the publisher database
Note: This was lifted from the Web repo and modified for Prefect 
"""

# move articles to the queue to be processed
MOVE_TO_PROCESSING_QUEUE_SQL = """
INSERT IGNORE 
    INTO `readitla_pub`.content_quality_score_beta_queue (publisher_id, content_id) 
        SELECT q.publisher_id, q.content_id 
            FROM `readitla_pub`.content_quality_score_queue q 
            LEFT JOIN `readitla_pub`.content c ON q.content_id = c.id 
                  AND q.publisher_id = c.publisher_id
"""

# Drop all records that were moved to our processing queue.
TRUNCATE_CONTENT_QUALITY_SQL = """
TRUNCATE TABLE `readitla_pub`.content_quality_score_queue
"""

DATA_FOR_SCORE_CALCULATION_SQL = """
SELECT 
    bq.publisher_id, bq.content_id, pcl.save_cnt, pcl.open_cnt, pcl.favorite_cnt, pcl.share_cnt 
    from `readitla_pub`.content_quality_score_beta_queue bq 
    left join `readitla_pub`.publisher_content_lifetime pcl on bq.content_id = pcl.content_id 
          and bq.publisher_id = pcl.publisher_id limit 100000
"""  # noqa: E501

INSERT_TO_CONTENT_V1_SQL = """
INSERT IGNORE 
    INTO `readitla_pub`.content_quality_score (publisher_id, content_id) 
    values (%s, %s)
"""

INSERT_TO_CONTENT_V2_SQL = """
INSERT IGNORE 
    INTO `readitla_pub`.content_quality_score_v2 (publisher_id, content_id) 
    VALUES (%s, %s)
"""

UPDATE_CONTENT_V1_SQL = """
UPDATE `readitla_pub`.content_quality_score 
    set save_cnt = %s, 
        open_uv = %s, 
        open_dv = %s, 
        fav_uv = %s, 
        fav_dv = %s, 
        share_uv = %s, 
        share_dv = %s, 
        open_score = round(%s,4), 
        favorite_score = round(%s,4), 
        share_score = round(%s,4), 
        total_score = round(%s,4) 
        where publisher_id = %s
              and content_id = %s
"""

UPDATE_CONTENT_V2_SQL = """
UPDATE `readitla_pub`.content_quality_score_v2 
    set save_cnt = %s, 
        open_uv = %s, 
        open_dv = %s, 
        fav_uv = %s, 
        fav_dv = %s, 
        share_uv = %s, 
        share_dv = %s, 
        open_score = round(%s,4), 
        favorite_score = round(%s,4), 
        share_score = round(%s,4), 
        total_score = round(%s,4) 
        where publisher_id = %s
              and content_id = %s
"""

DELETE_PROCESS_QUEUE_SQL = """
DELETE FROM `readitla_pub`.content_quality_score_beta_queue 
    where publisher_id = %s
    and content_id = %s
"""

# max signed int
# 2018-04-11 - We hit the max signed int with our item_ids. This broke tables did not have `unsigned` specified.  # noqa: E501
# The hack we are adding here creates a new content_quality_score table (_v2), fixes the column definitions  # noqa: E501
# and inserts new items into that table.
max_content_id = 2147483647
# score variables
loss_multiple = 9
inc_beta_var = float(1) / (1 + loss_multiple)
open_var = 1
fav_var = 5
share_var = 7


@task()
def transform(rows) -> list[dict[str, Union[float, int]]]:
    result = []

    for row in rows:
        publisher_id = int(row[0])
        content_id = int(row[1])

        if (
            row[2] is not None
            and row[3] is not None
            and row[4] is not None
            and row[5] is not None
        ):
            save_cnt = int(row[2])
            open_cnt = int(row[3])
            favorite_cnt = int(row[4])
            share_cnt = int(row[5])

            if save_cnt > 0 or open_cnt > 0 or favorite_cnt > 0 or share_cnt > 0:
                open_uv = float(open_cnt) + 2.2 + save_cnt * 0.05
                open_dv = (float(save_cnt) - open_cnt) + 7.8
                open_dv = open_dv if save_cnt >= open_cnt else (open_dv + open_uv) * 4
                fav_uv = float(favorite_cnt) + 0.2 + open_cnt * 0.02
                fav_dv = (float(open_cnt) - favorite_cnt) + 9.8
                fav_dv = fav_dv if open_cnt >= favorite_cnt else (fav_dv + fav_uv) * 4
                share_uv = float(share_cnt) + 0.2 + open_cnt * 0.02
                share_dv = (float(open_cnt) - share_cnt) + 9.8
                share_dv = (
                    share_dv if open_cnt >= share_cnt else (share_dv + share_uv) * 4
                )

                open_score = scipy.special.betaincinv(open_uv, open_dv, inc_beta_var)
                fav_score = scipy.special.betaincinv(fav_uv, fav_dv, inc_beta_var)
                share_score = scipy.special.betaincinv(share_uv, share_dv, inc_beta_var)

                total_score = (
                    open_score * open_var
                    + fav_score * fav_var
                    + share_score * share_var
                ) / (open_var + fav_var + share_var)

                result.append(
                    {
                        "save_cnt": save_cnt,
                        "open_uv": open_uv,
                        "open_dv": open_dv,
                        "fav_uv": fav_uv,
                        "fav_dv": fav_dv,
                        "share_uv": share_uv,
                        "share_dv": share_dv,
                        "open_score": open_score,
                        "favorite_score": fav_score,
                        "share_score": share_score,
                        "total_score": total_score,
                        "publisher_id": publisher_id,
                        "content_id": content_id,
                    }
                )
    return result


def insert_args(
    rows: list[dict[str, Union[float, int]]]
) -> list[tuple[float | int, float | int]]:
    return [(x["publisher_id"], x["content_id"]) for x in rows]


def delete_args(
    rows: list[dict[str, Union[float, int]]]
) -> list[tuple[float | int, float | int]]:
    return [(x["publisher_id"], x["content_id"]) for x in rows]


def update_args(rows: list[dict[str, Union[float, int]]]) -> list[tuple]:
    return [
        (
            x["save_cnt"],
            x["open_uv"],
            x["open_dv"],
            x["fav_uv"],
            x["fav_dv"],
            x["share_uv"],
            x["share_dv"],
            x["open_score"],
            x["favorite_score"],
            x["share_score"],
            x["total_score"],
            x["publisher_id"],
            x["content_id"],
        )
        for x in rows
    ]


@flow(
    retries=18,
    retry_delay_seconds=10,
    timeout_seconds=10 * 60,
    name="data-products.update-pocket-score-subflow",
)
async def load(
    rows: list[dict[str, Union[float, int]]], mysql_creds: MozSqlalchemyCredentials
):
    logger = get_run_logger()

    one_dict = [x for x in rows if x["content_id"] <= max_content_id]
    two_dict = [x for x in rows if x["content_id"] > max_content_id]

    if len(one_dict) > 0:
        logger.info("V1 dictionary length %s", len(one_dict))
        inserts = sqlalchemy_execute.map(
            statement=unmapped(INSERT_TO_CONTENT_V1_SQL),  # type: ignore
            params=insert_args(one_dict),  # type: ignore
            sqlalchemy_credentials=unmapped(mysql_creds),  # type: ignore
        )
        updates = sqlalchemy_execute.map(
            statement=unmapped(UPDATE_CONTENT_V1_SQL),  # type: ignore
            params=update_args(one_dict),  # type: ignore
            sqlalchemy_credentials=unmapped(mysql_creds),  # type: ignore
        )
        deletes = sqlalchemy_execute.map(
            statement=unmapped(DELETE_PROCESS_QUEUE_SQL),  # type: ignore
            params=delete_args(one_dict),  # type: ignore
            sqlalchemy_credentials=unmapped(mysql_creds),  # type: ignore
        )
        logger.info(
            "V1 inserts: %s, updates: %s, deletes: %s", inserts, updates, deletes
        )
    else:
        logger.info("No publisher records to process for the V1 process")

    if len(two_dict) > 0:
        logger.info("V2 dictionary length %s", len(two_dict))
        inserts = sqlalchemy_execute.map(
            statement=unmapped(INSERT_TO_CONTENT_V2_SQL),  # type: ignore
            params=insert_args(two_dict),  # type: ignore
            sqlalchemy_credentials=unmapped(mysql_creds),  # type: ignore
        )
        updates = sqlalchemy_execute.map(
            statement=unmapped(UPDATE_CONTENT_V2_SQL),  # type: ignore
            params=update_args(two_dict),  # type: ignore
            sqlalchemy_credentials=unmapped(mysql_creds),  # type: ignore
        )
        deletes = sqlalchemy_execute.map(
            statement=unmapped(DELETE_PROCESS_QUEUE_SQL),  # type: ignore
            params=delete_args(two_dict),  # type: ignore
            sqlalchemy_credentials=unmapped(mysql_creds),  # type: ignore
        )
        logger.info(
            "V2 inserts: %s, updates: %s, deletes: %s", inserts, updates, deletes
        )
    else:
        logger.info("No publisher records to process for the V2 process")


@flow()
async def update_pocket_score_data():
    mysql_creds = MozSqlalchemyCredentials()

    # move articles to the queue to be processed from their original holding queue
    move_to_beta_queue_task = await sqlalchemy_execute(
        statement=MOVE_TO_PROCESSING_QUEUE_SQL, sqlalchemy_credentials=mysql_creds
    )

    # Drop all articles from the holding queue since we moved them to our processing queue.  # noqa: E501
    truncate_content_quality_task = await sqlalchemy_execute(
        statement=TRUNCATE_CONTENT_QUALITY_SQL,
        sqlalchemy_credentials=mysql_creds,
        wait_for=[move_to_beta_queue_task],  # type: ignore
    )

    # Take the articles from our processing queue and get the data
    extract_result = await sqlalchemy_query(
        query=DATA_FOR_SCORE_CALCULATION_SQL,
        sqlalchemy_credentials=mysql_creds,
        wait_for=[truncate_content_quality_task],  # type: ignore
    )
    # Transform the articles we grabbed to have our new data
    transform_result = transform(extract_result)

    # Load the processed data back into MySQL with a content score!
    await load(rows=transform_result, mysql_creds=mysql_creds)


FLOW_SPEC = FlowSpec(
    flow=update_pocket_score_data,
    docker_env="base",
    deployments=[
        FlowDeployment(
            name="deployment",
            cron="*/60 * * * *",
            job_variables={
                "cpu": 2048,
                "memory": 4096,
            },
        ),
    ],
)


if __name__ == "__main__":
    import asyncio

    asyncio.run(update_pocket_score_data())  # type: ignore
