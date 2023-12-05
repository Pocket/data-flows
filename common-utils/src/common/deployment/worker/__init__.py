"""Deployment utilities module with tools for Prefect CI/CD"""
import glob
import logging
import os
import shlex
import sys
from importlib.machinery import SourceFileLoader
from pathlib import Path
from subprocess import PIPE, STDOUT, Popen
from typing import Any, Optional

import toml
import yaml
from common import find_pyproject_file, get_script_path
from prefect import Flow
from pydantic import BaseModel, FilePath, PrivateAttr, StrictStr, ValidationError
from slugify import slugify

# Create logger for module
LOGGER_NAME = __name__
LOGGER = logging.getLogger(LOGGER_NAME)

# script path for referencing bash scripts
SCRIPT_PATH = get_script_path()

# config for deploying flows and environments
DEPLOYMENT_TYPE = os.getenv("MOZILLA_PREFECT_DEPLOYMENT_TYPE", "dev")
AWS_REGION = os.getenv("DEFAULT_AWS_REGION", "us-east-1")
AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID")
PYPROJECT_FILE_PATH = os.path.expanduser(os.path.join(os.getcwd(), "pyproject.toml"))
FLOWS_PATH = Path(os.getenv("MOZILLA_FLOWS_PATH_OVERRIDE", "src"))
DEFAULT_WORK_POOL = os.getenv("MOZILLA_DEFAULT_WORK_POOL", "mozilla-aws-ecs-fargate")
DEPLOYMENT_BRANCH = os.getenv("MOZILLA_PREFECT_DEPLOYMENT_BRANCH", "dev-v2")
GIT_SHA = os.getenv("GIT_SHA", "dev")
DEFAULT_CPU = "512"
DEFAULT_MEMORY = "1024"


def run_command(command: str) -> str:
    """Helper function to execute shell commands.
    Provides stream logging of stdout and returns the last line
    for return values from scripts.

    Args:
        command (str): Shell command to run.

    Raises:
        Exception: Throws when exit code is not zero.

    Returns:
        str: Last line from stdout.
    """
    line = "Running Shell Command..."
    with Popen(
        command, stdout=PIPE, stderr=STDOUT, shell=True, executable="/bin/bash"
    ) as sub_process:
        for raw_line in iter(sub_process.stdout.readline, b""):  # type: ignore
            line = raw_line.decode("utf-8").rstrip()
            LOGGER.info(line)

        sub_process.wait()
        if sub_process.returncode:
            msg = "Command failed with exit code {}".format(
                sub_process.returncode,
            )
            LOGGER.error(msg)
            raise Exception(f"Shell Command Failed with last line: {line}!")
    return line


def get_image_name(project_name: str, env_name: str) -> str:
    return f"{project_name}-{standard_slugify(env_name)}"


def get_ecs_task_name(project_name: str, env_name: str) -> str:
    return f"{get_image_name(project_name, env_name)}-{DEPLOYMENT_TYPE}"


def get_ecs_image_name(project_name: str, env_name: str) -> str:
    return f"{AWS_ACCOUNT_ID}.dkr.ecr.{AWS_REGION}.amazonaws.com/data-flows-prefect-v2-envs:{get_image_name(project_name, env_name)}-{GIT_SHA}"  # noqa: E501


def get_ecs_task_arn(project_name: str, env_name: str) -> str:
    return f"arn:aws:ecs:{AWS_REGION}:{AWS_ACCOUNT_ID}:task-definition/{get_ecs_task_name(project_name, env_name)}"  # noqa: E501


def standard_slugify(string: str) -> str:
    """Helper function to create dash slugified strings.

    Args:
        string (str): String to slugify.

    Returns:
        str: Slugified string.
    """
    return slugify(string, separator="-").lower()


class FlowDockerEnv(BaseModel):
    """Model that represents a Prefect Docker environment."""

    env_name: StrictStr
    dockerfile_path: FilePath
    dependency_group: str = "main"
    python_version: str
    project_name: StrictStr
    _prefect_version: str = PrivateAttr()

    @property
    def image_name(self):
        return get_image_name(self.project_name, self.env_name)

    @property
    def ecs_task_name(self):
        return get_ecs_task_name(self.project_name, self.env_name)

    @property
    def ecs_image_name(self):
        return get_ecs_image_name(self.project_name, self.env_name)

    def __init__(self, **data) -> None:
        """Set prefect version on init."""
        super().__init__(**data)
        self._prefect_version = run_command(
            shlex.join([f"{SCRIPT_PATH}/get_prefect_version.sh", self.dependency_group])
        )

    def build_image(self) -> None:
        """Build a docker image using model attributes."""
        run_command(
            shlex.join(
                [
                    f"{SCRIPT_PATH}/build_image.sh",
                    self.image_name,
                    str(self.dockerfile_path),
                    self.dependency_group,
                    self.python_version,
                    self._prefect_version,
                ]
            )
        )

    def push_image(self, push_type: str = "aws"):
        config = {"aws": self.push_image_aws}
        config[push_type]()

    def push_image_aws(self):
        """Push built image to ECR

        Returns:
            str: Pushed ECR image name.
        """

        # push image to ECR using stored image name
        run_command(
            shlex.join(
                [
                    f"{SCRIPT_PATH}/push_aws_image.sh",
                    self.image_name,
                    AWS_ACCOUNT_ID,  # type: ignore
                    AWS_REGION,
                    GIT_SHA,
                ]
            )
        )

    def _handle_ecs_task_definition(self, ecs_client: object) -> str | None:
        """Register Task Defintition with AWS ECS as needed based on changes.

        Args:
            ecs_client (object): valid ecs client

        Returns:
            str: New or existing Task Definition ARN.
        """
        # set up default return value register flag
        # evaluations will set register to True as needed
        register = False
        final_task_def_arn = None
        # start building the task definition elements from FlowSpec and globals
        # these values plus other defaults will be the basis for task def diff analysis
        task_name = self.ecs_task_name
        image_name = self.ecs_image_name
        task_role_arn = f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/data-flows-prefect-{DEPLOYMENT_TYPE}-task-role"  # noqa: E501
        execution_role_arn = f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/data-flows-prefect-{DEPLOYMENT_TYPE}-exec-role"  # noqa: E501
        default_cpu = DEFAULT_CPU
        default_memory = DEFAULT_MEMORY
        default_container_name = "prefect"

        # set the compare keys
        # these are used to determine if we need a new task definition registered or not
        task_def_compare_keys = [
            "taskRoleArn",
            "executionRoleArn",
            "requiresCompatibilities",
            "cpu",
            "memory",
            "volumes",
        ]

        container_def_compare_keys = [
            "image",
            "environment",
            "secrets",
            "logConfiguration",
            "mountPoints",
        ]

        # create our new or initial version of the task definition
        new_task_def_dict = {
            "family": task_name,
            "taskRoleArn": task_role_arn,
            "executionRoleArn": execution_role_arn,
            "networkMode": "awsvpc",
            "containerDefinitions": [
                {
                    "name": default_container_name,
                    "image": image_name,
                    "environment": [
                        {"name": "DF_CONFIG_DEPLOYMENT_TYPE", "value": DEPLOYMENT_TYPE},
                    ],
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-create-group": "true",
                            "awslogs-group": default_container_name,
                            "awslogs-region": AWS_REGION,
                            "awslogs-stream-prefix": task_name,
                        },
                    },
                    "mountPoints": [
                        {
                            "sourceVolume": "prefect-scratch",
                            "containerPath": "/var/prefect-scratch",
                        }
                    ],
                }
            ],
            "requiresCompatibilities": [
                "FARGATE",
            ],
            "cpu": default_cpu,
            "memory": default_memory,
            "ephemeralStorage": {"sizeInGiB": 21},
            "volumes": [
                {
                    "name": "prefect-scratch",
                }
            ],
        }
        # check to see if we have any registered task definitions
        task_def_arns = ecs_client.list_task_definitions(  # type: ignore
            familyPrefix=task_name, maxResults=5
        )
        # if we have some then we treat as an existing task definition and start diff
        if task_def_arns["taskDefinitionArns"]:
            current_task_def_response = ecs_client.describe_task_definition(  # type: ignore  # noqa: E501
                taskDefinition=task_name
            )
            current_task_def = current_task_def_response["taskDefinition"]
            final_task_def_arn = current_task_def["taskDefinitionArn"]
            # if any of our top level keys have changed we mark as "register = True"
            if not all(
                new_task_def_dict.get(key) == current_task_def.get(key)
                for key in task_def_compare_keys
            ):
                LOGGER.info(
                    f"Task definition state has changed for {task_name},"
                    "registering new definition..."
                )
                register = True
            # if any container definition keys have changed we mark as "register = True"
            elif not all(
                new_task_def_dict.get("containerDefinitions", [{}])[0].get(key)
                == current_task_def.get("containerDefinitions", [{}])[0].get(key)
                for key in container_def_compare_keys
            ):
                LOGGER.info(
                    f"Task definition state has changed for {task_name}, "
                    "registering new definition..."
                )
                register = True
        # if no registered arns, we treat as brand new and mark  "register = True"
        else:
            LOGGER.info(
                f"Task definition not registered for {task_name}, "
                "registering new definition..."
            )
            register = True
        # if "register = True" we register a task definition
        if register:
            response = ecs_client.register_task_definition(  # type: ignore
                **new_task_def_dict
            )
            final_task_def_arn = response["taskDefinition"]["taskDefinitionArn"]
        else:
            LOGGER.info(
                f"Task definition state has not changed {task_name}, "
                "new revision not needed..."
            )
        # this will return the current or newly registered task definition version arn
        return final_task_def_arn


# model for pyproject metadata
class PrefectProject(BaseModel):
    """Model to hold Prefect project metadata"""

    project_name: str
    docker_envs: list[FlowDockerEnv]

    @property
    def docker_env_keys(self):
        return [x.env_name for x in self.docker_envs]

    def clone_project(self, gh_repo: str, branch: str, root_dir: str = "opt"):
        run_command(
            shlex.join(
                [
                    f"{SCRIPT_PATH}/clone_project.sh",
                    self.project_name,
                    gh_repo,
                    branch,
                    root_dir,
                ]
            )
        )

    def process_docker_envs(
        self, build_only: bool = False, push_type: str = "aws"
    ) -> None:
        """Method to process project docker envs from the project's
        pyproject.toml

        Args:
            build_only (bool, optional): Whether to run docker build only.
            Defaults to False.
        """
        ecs_client = object()
        if not build_only:
            if push_type == "aws":
                # only need AWS SDK on deployment
                from boto3 import session

                init_session = session.Session()
                ecs_client = init_session.client("ecs")
        for env in self.docker_envs:
            env.build_image()
            if not build_only:
                env.push_image(push_type)
                if push_type == "aws":
                    env._handle_ecs_task_definition(ecs_client)

    async def process_flow_specs(self) -> None:
        # start building the yaml elements

        gh_repo = run_command(shlex.join(["git", "ls-remote", "--get-url", "origin"]))

        base_pull = [
            {
                "prefect.deployments.steps.run_shell_script": {
                    "id": "clone_project",
                    "script": f"df-cli clone-project {gh_repo} {DEPLOYMENT_BRANCH}",
                    "stream_output": False,
                }
            },
            {
                "prefect.deployments.steps.set_working_directory": {
                    "directory": f"/opt/prefect/{self.project_name}"
                }
            },
        ]

        # create base list
        deployments = []

        missing_flow_specs = []
        flow_errors = []
        for name in glob.glob(os.path.join(FLOWS_PATH, "**/*_flow.py"), recursive=True):
            path_object = Path(name)
            file_name = path_object.name
            mod = file_name.split(".")[0]
            try:
                # add flows path to pythonpath for interproject shared libs
                pythonpath_addition = os.path.expanduser(
                    os.path.join(os.getcwd(), FLOWS_PATH)
                )
                sys.path.append(pythonpath_addition)
                # load FLOW_SPEC global from file
                fs = SourceFileLoader(mod, name).load_module().FLOW_SPEC
                if fs.is_agent:
                    continue
                for d in fs.deployments:
                    fd = {
                        "name": f"{d.name}-{DEPLOYMENT_TYPE}",
                        "tags": d.tags,
                        "parameters": d.parameters,
                        "enforce_parameter_schema": d.enforce_parameter_schema,
                        "work_pool": {
                            "name": f"{d.work_pool_name}-{DEPLOYMENT_TYPE}",
                            "work_queue_name": d.work_queue_name or "default",
                        },
                        "entrypoint": f"{name}:{fs.flow.fn.__name__}",
                    }
                    if x := d.cron:
                        fd["schedule"] = {
                            "cron": x,
                            "timezone": d.timezone,
                        }
                    if x := d.description:
                        fd["description"] = x
                    if x := d.version:
                        fd["version"] = x
                    if x := d.job_variables:
                        fd["work_pool"]["job_variables"] = x
                    if "aws" in d.work_pool_name:
                        fd["work_pool"]["job_variables"][
                            "task_definition_arn"
                        ] = get_ecs_task_arn(self.project_name, fs.docker_env)
                    deployments.append(fd)

            # catch errors and aggregate into groups for logging and raising
            except AttributeError as e:
                LOGGER.info(e)
                LOGGER.info(f"Flow {mod} does not have a FLOW_SPEC defined!")
                missing_flow_specs.append(mod)
            except (ValidationError, ValueError) as e:
                LOGGER.error(e)
                LOGGER.error(f"FLOW_SPEC for {mod} is not properly defined!")
                flow_errors.append(mod)
            except Exception as e:
                LOGGER.error(e)
                flow_errors.append(mod)
        if missing_flow_specs:
            LOGGER.info(
                f"The following flows are missing FLOW_SPEC "
                f"defintions: {missing_flow_specs}"
            )
            LOGGER.info("This is assumed as expected and will not throw an Exception.")
        # start building the deployment yaml

        prefect_flow_config = {
            "name": self.project_name,
            "pull": base_pull,
            "deployments": deployments,
        }

        with open("prefect.yaml", "w") as f:
            yaml.dump(prefect_flow_config, f)
        LOGGER.info(
            "All flows with proper FLOW_SPEC definitions "
            "have completed processing successfully!"
        )
        if flow_errors:
            raise Exception(f"The following flows failed processing: {flow_errors}")


# creating and running a helper function to extract project metadata.
def get_pyproject_metadata() -> PrefectProject:
    """Produce a PyProjectMetadata object from the pyproject.toml file.

    Returns:
        PyProjectMetadata: Model containing the pyproject metadata.
    """
    with open(find_pyproject_file()) as t:
        config = toml.load(t)
    project_name = standard_slugify(config["tool"]["poetry"]["name"])
    docker_config = config["tool"].get("prefect", {"docker": {}})["docker"]
    return PrefectProject(
        project_name=project_name,
        docker_envs=[
            FlowDockerEnv(env_name=k, project_name=project_name, **v)
            for k, v in docker_config.items()
        ],
    )


class FlowDeployment(BaseModel):
    name: str
    cron: str | None = None
    timezone: str = "UTC"
    parameters: Optional[dict] = {}
    description: Optional[str] = None
    tags: Optional[list[str]] = []
    version: Optional[str] = None
    enforce_parameter_schema: bool = False
    work_pool_name: Optional[str] = DEFAULT_WORK_POOL
    work_queue_name: Optional[str] = "default"
    job_variables: Optional[dict[str, Any]] = {}


class FlowSpec(BaseModel):
    """This is the object we use to deploy a flow."""

    flow: Flow
    """Flow funciton object."""
    docker_env: StrictStr
    """docker environment name from pyproject.toml, which is the 
    key name in '[tool.prefect.docker.<>]'"""
    deployments: list[FlowDeployment] = []
    """list of deployment objects to register with Prefect"""
    is_agent: bool = False

    def __init__(self, **data):
        super().__init__(**data)
        pm = get_pyproject_metadata()
        self.flow.name = f"{pm.project_name}.{self.flow.name}"
        if self.docker_env not in pm.docker_env_keys:
            raise ValueError(
                f"Docker env '{self.docker_env}' does not exist in pyproject.toml.  "
                "It must be the key name in a "
                "'[tool.prefect.envs.<key name>]' configuration."
            )

    class Config:
        arbitrary_types_allowed = True
