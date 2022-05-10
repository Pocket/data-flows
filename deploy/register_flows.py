#!/usr/bin/env python
import json
import os
from os import environ
from typing import Callable

import prefect
from prefect.backend import FlowView
from prefect.run_configs import RunConfig, ECSRun
from prefect.storage import Storage, Local
from prefect.client import Client


# Takes in a path to the flow, and returns a storage object.
from utils.graphql import get_flow_group_id_by_flow_id

STORAGE_FACTORY_TYPE = Callable[[str], Storage]


def create_local_storage(flow_path: str) -> Storage:
    """
    Creates a local storage object for use in ECS.
    :param flow_path: Path to the flow
    :return:
    """
    return Local(
        stored_as_script=True,  # We store the flows in the Docker image
        path=flow_path,  # Direct path to the storage in the Docker container
        add_default_labels=False,  # Don't label the flow with the local machine name
    )


class FlowDeployment:
    """
    Discovers, builds, and registers flows with Prefect Cloud.
    """

    def __init__(self, project_name: str, storage_factory: STORAGE_FACTORY_TYPE, run_config: RunConfig, build: bool):
        """
        :param project_name: Name of the Prefect project to deploy flows to.
        :param storage_factory: Function that returns a Prefect storage object and accepts the flow path as an argument.
                                https://docs.prefect.io/orchestration/execution/storage_options.html
        :param run_config: Prefect run config: https://docs.prefect.io/orchestration/flow_config/run_configs.html
        :param build: If false, the Prefect Flows will not be built into the storage, but only registered.
        """
        self.project_name = project_name
        self.storage_factory = storage_factory
        self.run_config = run_config
        self.build = build

    def register_flow(self, file_path: str) -> str:
        """
        Register a single flow with Prefect
        :param file_path: Path of the Python file where the flow is defined.
        :return flow_id of the flow that was registered
        """
        flow = prefect.utilities.storage.extract_flow_from_file(file_path)

        storage = self.storage_factory(file_path)
        if not self.build:
            # If Prefect builds the flow, it automatically adds the flow to the storage. Otherwise we have to do it.
            storage.add_flow(flow)
        flow.storage = storage

        flow.run_config = self.run_config

        # flow.register builds the flow and registers it with Prefect.
        flow_id = flow.register(self.project_name, build=self.build)
        return flow_id

    def update_flow_readme(self, flow_id: str, markdown_path: str):
        markdown_header = f"_This readme was generated from {markdown_path}. " \
                          f"Make edits in the Github repo, and NOT in the Prefect UI, to prevent loosing changes._\n\n"

        with open(markdown_path) as f:
            markdown_content = f.read()

        mutation = {
            "mutation($input: set_flow_group_description_input!)": {
                "set_flow_group_description(input: $input)": {"success", "error"}
            }
        }

        result = Client().graphql(
            mutation,
            variables={
                "input": {
                    "flow_group_id": get_flow_group_id_by_flow_id(flow_id),
                    "description": markdown_header + markdown_content,
                }
            },
        )

        print(result)

    def register_all_flows(self, flows_path: str):
        """
        Discover all flows in the given directory, including recursively, and register them.
        :param flows_path: Directory path containing Prefect flows.
        """
        for flow_path in self._get_all_python_files(flows_path):
            print(f"Registering {flow_path}")
            flow_id = self.register_flow(flow_path)

            markdown_path = os.path.splitext(flow_path)[0] + '.md'
            if os.path.exists(markdown_path):
                print(f"Updating flow readme {markdown_path}")
                self.update_flow_readme(flow_id, markdown_path)

    def _get_all_python_files(self, dir_path: str) -> str:
        """
        Get all Python files in the given directory, including sub-directories.
        :param dir_path: Directory path containing Python files.
        :return: List of full paths for all Python files in dir_path
        """
        for root, dirs, files in os.walk(dir_path):
            for file in files:
                # Ignore __pycache__ and other non-python files by filtering on .py extension.
                if file.endswith(".py") and file != '__init__.py':
                    yield os.path.join(root, file)

# This script is executed in CodeBuild using buildspec_register_flows.yml
if __name__ == "__main__":
    # TODO: It would be cleaner to use command line arguments instead of loading values from environment variables.
    PREFECT_PROJECT_NAME = environ['PREFECT_PROJECT_NAME']
    PREFECT_TASK_DEFINITION_ARN = environ['PREFECT_TASK_DEFINITION_ARN']
    ENVIRONMENT = environ['ENVIRONMENT']

    FLOWS_PATH = os.path.join(environ['DATA_FLOWS_SOURCE_DIR'], 'flows/')

    FlowDeployment(
        project_name=PREFECT_PROJECT_NAME,
        storage_factory=create_local_storage,
        run_config=ECSRun(
            labels=[PREFECT_PROJECT_NAME],
            task_definition_arn=PREFECT_TASK_DEFINITION_ARN,
            env={'ENVIRONMENT': ENVIRONMENT, 'PREFECT_PROJECT_NAME': PREFECT_PROJECT_NAME},
        ),
        build=False,  # The flows are included in the Docker image, so don't need to be built by Prefect.
    ).register_all_flows(FLOWS_PATH)
