#!/usr/bin/env python

import os
from os import environ

import prefect
from prefect.run_configs import RunConfig, ECSRun
from prefect.storage import Storage, S3


class DeployAgent:
    def __init__(self, project_name: str, storage: Storage, run_config: RunConfig):
        self.project_name = project_name
        self.storage = storage
        self.run_config = run_config

    def register_flow(self, file_path: str):
        flow = prefect.utilities.storage.extract_flow_from_file(file_path)
        flow.storage = self.storage
        flow.run_config = self.run_config
        #flow.register(self.project_name)

    def register_all_flows(self, flows_path: str):
        for flow_path in self.get_all_python_files(flows_path):
            print(f"Registering {flow_path}")
            self.register_flow(flow_path)

    def get_all_python_files(self, dir_path) -> str:
        for root, dirs, files in os.walk(dir_path):
            for file in files:
                # Ignore __pycache__ and other non-python files by filtering on .py extension.
                if file.endswith(".py"):
                    yield os.path.join(root, file)


if __name__ == "__main__":
    PREFECT_PROJECT_NAME = environ['PREFECT_PROJECT_NAME']
    PREFECT_STORAGE_BUCKET = environ['PREFECT_STORAGE_BUCKET']
    PREFECT_IMAGE = environ['PREFECT_IMAGE']
    PREFECT_TASK_ROLE_ARN = environ['PREFECT_RUN_TASK_ROLE']
    FLOWS_PATH = r'./src/flows'

    # TODO: Switch to local storage for only running deployed flows
    DeployAgent(
        PREFECT_PROJECT_NAME,
        S3(
            bucket=PREFECT_STORAGE_BUCKET,
            add_default_labels=False,
        ),
        ECSRun(
            labels=[PREFECT_PROJECT_NAME],
            image=PREFECT_IMAGE,
            task_role_arn=PREFECT_TASK_ROLE_ARN,
        )
    ).register_all_flows(FLOWS_PATH)
