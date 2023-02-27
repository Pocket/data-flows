"""Deployment utilities module with tools for Prefect CI/CD"""
import logging
import os
from subprocess import PIPE, STDOUT, Popen
from pathlib import Path

import toml
from boto3 import session
from prefect.filesystems import S3
from pydantic import BaseModel, FilePath, PrivateAttr, StrictStr, DirectoryPath

# Create logger
LOGGER_NAME = __name__
LOGGER = logging.getLogger(LOGGER_NAME)

# script path for referencing bash scripts
SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))

# environment config for CI/CD
ENVIRONMENT_TYPE = os.getenv("PREFECT_ENVIRONMENT_TYPE", "dev")


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
    with Popen(command, stdout=PIPE, stderr=STDOUT, shell=True) as sub_process:
        for raw_line in iter(sub_process.stdout.readline, b""):
            line = raw_line.decode("utf-8").rstrip()
            LOGGER.info(line)

        sub_process.wait()
        if sub_process.returncode:
            msg = "Command failed with exit code {}".format(
                sub_process.returncode,
            )
            LOGGER.error(msg)
            raise Exception("Shell Command Failed!")
    return line


def get_aws_account_id() -> str:
    """Helper function to get current AWS account id.

    Returns:
        str: Accout id as string.
    """
    init_session = session.Session()
    sts = init_session.client("sts")
    return sts.get_caller_identity()["Account"]


class FlowDockerEnv(BaseModel):
    """Model that represents a Prefect Docker environment."""

    project_name: StrictStr
    env_name: StrictStr
    dockerfile_path: FilePath
    python_version: StrictStr
    docker_build_context: DirectoryPath = Path(".")
    _image_name: str = PrivateAttr()

    def __init__(self, **data) -> None:
        """Set the _image_name private attribute."""
        super().__init__(**data)
        image_name = f"{self.project_name}-{self.env_name}-py-{self.python_version}"
        self._image_name = image_name

    def build_image(self) -> None:
        """Build a docker image using model attributes."""
        image_name = self._image_name
        dockerfile_path = self.dockerfile_path
        python_version = self.python_version
        docker_build_context = self.docker_build_context
        run_command(
            f"{SCRIPT_PATH}/build_image.sh {image_name} {dockerfile_path} {python_version} {docker_build_context}"
        )

    def push_image(self, account_id: str) -> str:
        """Push built image to ECR

        Args:
            account_id (str): Account id for building ECR URL.

        Returns:
            str: Pushed ECR image name.
        """
        image_name = self._image_name
        pushed_image = run_command(
            f"{SCRIPT_PATH}/push_image.sh {image_name} {account_id}"
        )
        return pushed_image


class ProjectEnv(BaseModel):
    """Model that represents the project environment.
    This will store the list of docker envs to build and push.
    """

    project_name: StrictStr
    docker_envs: list[FlowDockerEnv]

    def _push_environments(self, build_only: bool = False) -> None:
        """_summary_

        Args:
            build_only (bool, optional): _description_. Defaults to False.
        """
        account_id = get_aws_account_id()
        for env in self.docker_envs:
            env.build_image()
            if not build_only:
                env.push_image(account_id)

    def _create_file_systems(self, build_only: bool = False) -> None:
        if not build_only:
            project_name = self.project_name
            for deployment_type in ["test", "live"]:
                bucket_name = (
                    f"data-flows-prefect-fs-{ENVIRONMENT_TYPE}-{deployment_type}"
                )
                block_name = f"{project_name}-{ENVIRONMENT_TYPE}-{deployment_type}"
                flows_fs = S3(bucket_path=f"{bucket_name}/{project_name}")
                result = flows_fs.save(block_name, overwrite=True)
                LOGGER.info(f"Filesystem block: {block_name} created with id: {result}")

    def process(self, build_only: bool = False) -> None:
        self._push_environments(build_only)
        self._create_file_systems(build_only)


def process_project_env(build_only: bool = False) -> None:
    """Helper function to process envs and related data from project's
    pyproject.toml

    Args:
        build_only (bool, optional): Whether to run docker build only. Defaults to False.
    """
    with open("pyproject.toml") as t:
        config = toml.load(t)
    project_name = config["tool"]["poetry"]["name"]
    envs = []
    for k, v in config["tool"].get("prefect", {"envs": {}})["envs"].items():
        env = FlowDockerEnv(project_name=project_name, env_name=k, **v)
        envs.append(env)
    project_env = ProjectEnv(project_name=project_name, docker_envs=envs)
    project_env.process(build_only)
