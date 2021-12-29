#!/usr/bin/env python

from os import environ
import prefect
from prefect.run_configs import RunConfig, ECSRun
from prefect.storage import Storage, S3
import os


class DeployAgent():
    def __init__(self, project_name: str, storage: Storage, run_config: RunConfig):
        self.project_name = project_name
        self.storage = storage
        self.run_config = run_config

    def register_flow(self, file_path: str):
        flow = prefect.utilities.storage.extract_flow_from_file(file_path)
        flow.storage = self.storage
        flow.run_config = self.run_config
        flow.register(self.project_name)

    def register_all_flows(self, flows_path: str):
        for filename in os.listdir(flows_path):
            print(f"Registering {filename}")
            self.register_flow(os.path.join(flows_path, filename))


ENVIRONMENT_NAME = 'Prod'  # environ.get('ENVIRONMENT')
PREFECT_IMAGE = '996905175585.dkr.ecr.us-east-1.amazonaws.com/dataflows-prod-app:latest'  # environ.get('PREFECT_IMAGE')
PREFECT_TASK_ROLE_ARN = 'arn:aws:iam::996905175585:role/DataFlows-Prod-RunTaskRole'  # environ.get('PREFECT_TASK_ROLE_ARN')
# PREFECT_EXECUTION_ROLE_ARN = 'arn:aws:iam::12345678:role/prefect-ecs'
PREFECT_STORAGE_BUCKET = f"pocket-dataflows-storage-{ENVIRONMENT_NAME.lower()}"
PREFECT_PROJECT_NAME = "prefect-tutorial"
FLOWS_PATH = r'./src/flows'

# TODO: Switch to local storage for only running deployed flows
DeployAgent(PREFECT_PROJECT_NAME,
            S3(
                bucket=PREFECT_STORAGE_BUCKET,
                add_default_labels=False,
            ),
            ECSRun(
                labels=[ENVIRONMENT_NAME],
                image=PREFECT_IMAGE,
                task_role_arn=PREFECT_TASK_ROLE_ARN,
                # execution_role_arn=PREFECT_EXECUTION_ROLE_ARN,
            )).register_all_flows(FLOWS_PATH)
