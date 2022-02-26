from prefect import Flow, task

from api_clients.braze import BrazeClient, UserTracking, UserAttributes

FLOW_NAME = "example_braze_update_flow"


@task
def update_user_email_addresses():
    user_tracking = UserTracking(
        attributes=[UserAttributes(external_id='mathijs-1234', email='mathijs@example.com')],
    )

    BrazeClient().track_users(user_tracking=user_tracking)


with Flow("s3download_flow") as flow:
    update_user_email_addresses()

if __name__ == "__main__":
    flow.run()
