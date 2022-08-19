import scipy.special
from prefect import Flow, task

from api_clients.pocket_publisher_database_execute import PocketPublisherDatabaseExecute
from api_clients.pocket_publisher_database_query import PocketPublisherDatabaseQuery
from utils.flow import get_flow_name, get_interval_schedule

'''
Load and update the content quality score in the publisher database
Note: This was lifted from the Web repo and modified for Prefect 
'''
# Setting flow variables
FLOW_NAME = get_flow_name(__file__)

# move articles to beta queue
MOVE_TO_BETA_QUEUE_SQL = """
INSERT IGNORE 
    INTO content_quality_score_beta_queue (publisher_id, content_id) 
        SELECT q.publisher_id, q.content_id 
            FROM content_quality_score_queue q 
            LEFT JOIN content c ON q.content_id = c.id 
                  AND q.publisher_id = c.publisher_id
"""

TRUNCATE_CONTENT_QUALITY_SQL = """
TRUNCATE TABLE content_quality_score_queue
"""

DATA_FOR_SCORE_CALCULATION_SQL = """
SELECT 
    bq.publisher_id, bq.content_id, pcl.save_cnt, pcl.open_cnt, pcl.favorite_cnt, pcl.share_cnt 
    from `readitla_pub`.content_quality_score_beta_queue bq 
    left join `readitla_pub`.publisher_content_lifetime pcl on bq.content_id = pcl.content_id 
          and bq.publisher_id = pcl.publisher_id limit 100
"""

INSERT_TO_CONTENT_V1_SQL = """
INSERT IGNORE 
    INTO `readitla_pub`.content_quality_score (publisher_id, content_id) 
    values ({publisher_id}, {content_id})
"""

INSERT_TO_CONTENT_V2_SQL = """
INSERT IGNORE 
    INTO `readitla_pub`.content_quality_score_v2 (publisher_id, content_id) 
    values ({publisher_id}, {content_id})
"""

UPDATE_CONTENT_V1_SQL = """
UPDATE `readitla_pub`.content_quality_score 
    set save_cnt = {save_cnt}, 
        open_uv = {open_uv}, 
        open_dv = {open_dv}, 
        fav_uv = {fav_uv}, 
        fav_dv = {fav_dv}, 
        share_uv = {share_uv}, 
        share_dv = {share_dv}, 
        open_score = round({open_score},4), 
        favorite_score = round({fav_score},4), 
        share_score = round({share_score},4), 
        total_score = round({total_score},4) 
        where publisher_id = {publisher_id}
              and content_id = {content_id}
"""

UPDATE_CONTENT_V2_SQL = """
UPDATE `readitla_pub`.content_quality_score_v2 
    set save_cnt = {save_cnt}, 
        open_uv = {open_uv}, 
        open_dv = {open_dv}, 
        fav_uv = {fav_uv}, 
        fav_dv = {fav_dv}, 
        share_uv = {share_uv}, 
        share_dv = {share_dv}, 
        open_score = round({open_score},4), 
        favorite_score = round({fav_score},4), 
        share_score = round({share_score},4), 
        total_score = round({total_score},4) 
        where publisher_id = {publisher_id}
              and content_id = {content_id}
"""

DELETE_BETA_QUEUE_SQL = """
DELETE FROM `readitla_pub`.content_quality_score_beta_queue 
    where publisher_id = {publisher_id} 
    and content_id = {content_id}
"""

# max signed int
# 2018-04-11 - We hit the max signed int with our item_ids. This broke tables did not have `unsigned` specified.
# The hack we are adding here creates a new content_quality_score table (_v2), fixes the column definitions
# and inserts new items into that table.
max_content_id = 2147483647
# score variables
loss_multiple = 9
inc_beta_var = float(1) / (1 + loss_multiple)
open_var = 1
fav_var = 5
share_var = 7


@task(timeout=5*60)
def while_loop():
    counter = 0
    query = PocketPublisherDatabaseQuery()
    execute = PocketPublisherDatabaseExecute()

    # loop through array of publisher_ids

    while True:
        # query for data needed to calculate score
        data = query.run(
            query=DATA_FOR_SCORE_CALCULATION_SQL,
            fetch='all'
        )

        if not data:
            break

        # loop through each row in data
        for row in data:

            publisher_id = int(row[0])
            content_id = int(row[1])

            if row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None:
                save_cnt = int(row[2])
                open_cnt = int(row[3])
                favorite_cnt = int(row[4])
                share_cnt = int(row[5])

                if save_cnt > 0 or open_cnt > 0 or favorite_cnt > 0 or share_cnt > 0:

                    if content_id > max_content_id:
                        execute.run(
                            query=INSERT_TO_CONTENT_V2_SQL.format(publisher_id=publisher_id, content_id=content_id)
                        )
                    else:
                        execute.run(
                            query=INSERT_TO_CONTENT_V1_SQL.format(
                                publisher_id=publisher_id, content_id=content_id)
                        )

                    open_uv = float(open_cnt) + 2.2 + save_cnt * 0.05
                    open_dv = (float(save_cnt) - open_cnt) + 7.8
                    open_dv = open_dv if save_cnt >= open_cnt else (open_dv + open_uv) * 4
                    fav_uv = float(favorite_cnt) + 0.2 + open_cnt * 0.02
                    fav_dv = (float(open_cnt) - favorite_cnt) + 9.8
                    fav_dv = fav_dv if open_cnt >= favorite_cnt else (fav_dv + fav_uv) * 4
                    share_uv = float(share_cnt) + 0.2 + open_cnt * 0.02
                    share_dv = (float(open_cnt) - share_cnt) + 9.8
                    share_dv = share_dv if open_cnt >= share_cnt else (share_dv + share_uv) * 4

                    open_score = scipy.special.betaincinv(open_uv, open_dv, inc_beta_var)
                    fav_score = scipy.special.betaincinv(fav_uv, fav_dv, inc_beta_var)
                    share_score = scipy.special.betaincinv(share_uv, share_dv, inc_beta_var)

                    total_score = (open_score * open_var + fav_score * fav_var + share_score * share_var) / (
                            open_var + fav_var + share_var)

                    update_vars = {'save_cnt': save_cnt, 'open_uv': open_uv, 'open_dv': open_dv, 'fav_uv': fav_uv,
                                   'fav_dv': fav_dv, 'share_uv': share_uv, 'share_dv': share_dv,
                                   'open_score': open_score, 'fav_score': fav_score, 'share_score': share_score,
                                   'total_score': total_score, 'publisher_id': publisher_id, 'content_id': content_id}

                    if content_id > max_content_id:
                        execute.run(query=UPDATE_CONTENT_V2_SQL.format(**update_vars))
                    else:
                        execute.run(query=UPDATE_CONTENT_V1_SQL.format(**update_vars))

            execute.run(
                query=DELETE_BETA_QUEUE_SQL.format(publisher_id=publisher_id, content_id=content_id)
            )

            counter += 1


with Flow(FLOW_NAME, schedule=get_interval_schedule(minutes=60)) as flow:
    move_to_beta_queue_task = PocketPublisherDatabaseExecute()(
        query=MOVE_TO_BETA_QUEUE_SQL
    )

    truncate_content_quality_task = PocketPublisherDatabaseExecute()(
        query=TRUNCATE_CONTENT_QUALITY_SQL
    ).set_upstream(
        move_to_beta_queue_task,  # Users creation needs to happen first
    )

    while_loop(upstream_tasks=[move_to_beta_queue_task, truncate_content_quality_task])

if __name__ == "__main__":
    flow.run()
