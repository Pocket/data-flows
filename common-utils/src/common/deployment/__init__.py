"""Deployment utilities module with tools for Prefect CI/CD"""
import glob
import json
import logging
import os
from importlib.machinery import SourceFileLoader
from pathlib import Path
from subprocess import PIPE, STDOUT, Popen
from typing import Any, Literal

import toml
from boto3 import session
from prefect import Flow
from prefect.filesystems import S3
from prefect.server.schemas.schedules import (
    SCHEDULE_TYPES,
    CronSchedule,
    IntervalSchedule,
    RRuleSchedule,
)
from prefect_aws.ecs import ECSTask
from pydantic import (
    BaseModel,
    DirectoryPath,
    Field,
    FilePath,
    PrivateAttr,
    StrictStr,
    validator,
)
from pydantic.error_wrappers import ValidationError
from slugify import slugify

# Create logger
LOGGER_NAME = __name__
LOGGER = logging.getLogger(LOGGER_NAME)

# script path for referencing bash scripts
SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))

# environment config for CI/CD
ENVIRONMENT_TYPE = os.getenv("PREFECT_ENVIRONMENT_TYPE", "dev").lower()

# config for deploying task definitions and flows
DEPLOYMENT_TYPE = os.getenv("PREFECT_DEPLOYMENT_TYPE", "test").lower()
GIT_SHA = os.getenv("CIRCLE_SHA1", "dev")[0:7]
AWS_REGION = os.getenv("DEFAULT_AWS_REGION", "us-east-1").lower()

# config for supporting using pyproject.toml in flow spec validation
# with support for circleci working directory.
CIRCLE_CWD = os.path.join(
    os.getenv("CIRCLE_WORKING_DIRECTORY", os.getcwd()), "pyproject.toml"
)
PYPROJECT_PATH = os.getenv("PREFECT_PYPROJECT_PATH", CIRCLE_CWD).lower()

# config to disable validation on FlowSpec for flow execution environments
DISABLE_FLOW_SPEC = os.getenv("PREFECT_DISABLE_FLOW_SPEC", "false").lower()


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
        for raw_line in iter(sub_process.stdout.readline, b""):  # type: ignore
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


def standard_slugify(string: str) -> str:
    """Helper function to create dash slugified strings.

    Args:
        string (str): String to slugify.

    Returns:
        str: Slugified string.
    """
    return slugify(string, separator="-").lower()


def create_image_name_str(project_name: str, env_name: str, python_version: str) -> str:
    """Helper function to create image name to enable resuability.

    Args:
        project_name (str): Slugified Project name from pyproject.toml.
        env_name (str): Slugified Docker env name.
        python_version (str): Python version.

    Returns:
        str: _description_
    """
    return f"{standard_slugify(project_name)}-{standard_slugify(env_name)}-py-{python_version}"


def get_aws_account_id() -> str:
    """Helper function to get current AWS account id.

    Returns:
        str: Accout id as string.
    """
    init_session = session.Session()
    sts = init_session.client("sts")
    return sts.get_caller_identity()["Account"]


def get_flow_folder(flow_path: Path) -> str:
    """Helper function to create a slugified string from the flow folder.
    Data flows are mean to be a Python file at the root of a 'flow folder'.
    We use this slugified name for storing the flow files in remote storage.

    Args:
        flow_path (Path): Path object for relative path of Python flow file.

    Returns:
        str: Slugified string from flow folder.
    """
    return standard_slugify(flow_path.parts[-2:-1][0])


class FlowDockerEnv(BaseModel):
    """Model that represents a Prefect Docker environment."""

    project_name: StrictStr
    env_name: StrictStr
    dockerfile_path: FilePath
    python_version: Literal["3.10"]
    docker_build_context: DirectoryPath = Path(".")
    _image_name: str = PrivateAttr()

    def build_image(self) -> None:
        """Build a docker image using model attributes."""
        image_name = create_image_name_str(
            self.project_name, self.env_name, self.python_version
        )
        self._image_name = image_name
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


class ProjectEnvs(BaseModel):
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
        """_summary_

        Args:
            build_only (bool, optional): Switch to only run if True. Defaults to False.
        """
        if not build_only:
            project_name = standard_slugify(self.project_name)
            for deployment_type in ["test", "live"]:
                bucket_name = (
                    f"data-flows-prefect-fs-{ENVIRONMENT_TYPE}-{deployment_type}"
                )
                block_name = f"{project_name}-{ENVIRONMENT_TYPE}-{deployment_type}"
                flows_fs = S3(bucket_path=f"{bucket_name}/{project_name}")
                result = flows_fs.save(block_name, overwrite=True)
                LOGGER.info(
                    f"Filesystem block: {block_name} created with id: {result}..."
                )

    def process(self, build_only: bool = False) -> None:
        """Method to run project envs workflow

        Args:
            build_only (bool, optional): Swtich to pass to methods. Defaults to False.
        """
        self._push_environments(build_only)
        self._create_file_systems(build_only)


class FlowSecret(BaseModel):
    """Model used to define an ECS Task envar that sources from Secrets Manager.
    The secret_name must be only the name and not the full ARN.
    The deployment process will create the ARN for you.
    """

    envar_name: StrictStr
    secret_name: StrictStr


class FlowDeployment(BaseModel):
    """Model that describes an abstracted flow deployment."""

    deployment_name: str = Field(
        ...,
        description="Name of the deployment, which will show as flow-name/deployment-name",
    )
    cpu: str = Field(
        "256", description="CPU amount that this deployment's flow runs should use."
    )
    memory: str = Field(
        "512", description="Memory amount that this deployment's flow runs should use."
    )
    parameters: dict[str, Any] = Field(
        {},
        description="Dictionary of parameter values to pass to this deployment's flow runs.",
    )
    schedule: SCHEDULE_TYPES = Field(None, description="Schedule configuration for this deployment using Prefect Schedule Objects.")  # type: ignore

    def _get_schedule_arg(self):
        """Helper function to translate Prefect Schedule Object into a command line argument.

        Returns:
            str: Command line argument to use for setting the deployment's schedule.
        """
        schedule = self.schedule
        if isinstance(schedule, CronSchedule):
            return f"--cron {schedule.cron}"
        elif isinstance(schedule, RRuleSchedule):
            return f"--rrule {schedule.rrule}"
        elif isinstance(schedule, IntervalSchedule):
            return f"--interval {schedule.interval.seconds}"
        else:
            return ""

    def push_deployment(
        self,
        project_name: str,
        storage_path: str,
        infrastructure: str,
        flow_path: Path,
        flow_function_name: str,
        skip_upload: bool,
    ) -> None:
        """This method will push a Prefect deployment to Prefect using the deployment cli.

        Args:
            project_name (str): Poetry project name so that we can use in tags.
            storage_block_path (str): Remote storage block path to use for flow files.
            infrastructure (str): ECS Task block name to use for the flow execution environment.
            flow_path (Path): Relative path to the flow python file.
            flow_function_name: (str): Name of the flow function to deploy.
            skip_upload (bool): Whether to skip upload. Will be set to True after first deployment pushed for flow.
        """
        deployment_name = standard_slugify(self.deployment_name)
        flow_file_name = flow_path.name
        overrides = f"--override cpu={self.cpu} --override memory={self.memory}"
        schedule = self._get_schedule_arg()
        params = f"'{json.dumps(self.parameters)}'"
        skip_upload_flag = ""
        if skip_upload:
            skip_upload_flag = "--skip-upload"
        run_command(
            f"""export PREFECT_PYPROJECT_PATH={PYPROJECT_PATH} && \\
        pushd {flow_path.parent} && \\
        prefect deployment build {flow_file_name}:{flow_function_name} \\
        -n {deployment_name} \\
        -sb s3/{storage_path} \\
        -ib ecs-task/{infrastructure} \\
        {overrides} \\
        -q prefect-v2-queue-{ENVIRONMENT_TYPE}-{DEPLOYMENT_TYPE} \\
        -v {GIT_SHA} \\
        --params {params} \\
        -t {project_name} -t {get_flow_folder(flow_path)} \\
        -a \\
        {schedule} {skip_upload_flag} && \\
        popd"""
        )
        LOGGER.info(
            f"Deployment: {deployment_name} for flow: {flow_path} applied successfully..."
        )


class FlowSpec(BaseModel):
    """This is the object we use to deploy a flow."""

    flow: Flow = Field(..., description="Flow funciton object.")
    secrets: list[FlowSecret] = Field(
        [], description="List of FlowSecret objects to pass to task definition."
    )
    docker_env: StrictStr = Field(
        ...,
        description="docker environment name from pyproject.toml, which is the key name in '[tool.prefect.envs.base]'",
    )
    ephemeral_storage_gb: int = 20
    deployments: list[FlowDeployment] = []
    _slugified_flow_name: StrictStr = PrivateAttr()
    _project_name: StrictStr = PrivateAttr()

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data) -> None:
        """Set the private fields on intansiation.
        DISABLE_FLOW_SPEC will be set to true to disable validation for ECS execution.
        """
        if DISABLE_FLOW_SPEC != "true":
            super().__init__(**data)
            with open(PYPROJECT_PATH) as t:
                config = toml.load(t)
            project_name = standard_slugify(config["tool"]["poetry"]["name"])
            self._project_name = project_name
            self.flow.name = f"{project_name}.{standard_slugify(self.flow.fn.__name__)}"

    @validator("docker_env")
    def docker_env_must_be_registered(cls, v):
        with open(PYPROJECT_PATH) as t:
            config = toml.load(t)
        docker_envs = [
            k for k in config["tool"].get("prefect", {"envs": {}})["envs"].keys()
        ]
        if v not in docker_envs:
            raise ValueError(
                "Docker env does not exist in pyproject.toml.  It must be the key name in a '[tool.prefect.envs.<key name>]' configuration."
            )
        else:
            return v

    def _create_ecs_task_block(self):
        """This method will create the ECS Task block as supported by common-utils."""
        slugified_flow_name = standard_slugify(self.flow.name)
        self._slugified_flow_name = slugified_flow_name
        task_name = f"{slugified_flow_name}-{DEPLOYMENT_TYPE}"
        block_name = f"{slugified_flow_name}-{ENVIRONMENT_TYPE}-{DEPLOYMENT_TYPE}"
        account_id = get_aws_account_id()
        ecs_block = ECSTask(
            name=block_name,
            family=task_name,
            image=self.docker_env,
            cpu="256",
            memory="512",
            stream_output=True,
            configure_cloudwatch_logs=True,
            cluster=f"prefect-v2-agent-{ENVIRONMENT_TYPE}-{DEPLOYMENT_TYPE}",
            execution_role_arn=f"arn:aws:iam::{account_id}:role/data-flows-prefect-{DEPLOYMENT_TYPE}-exec-role",
            task_role_arn=f"arn:aws:iam::{account_id}:role/data-flows-prefect-{DEPLOYMENT_TYPE}-task-role",
            task_definition={
                "containerDefinitions": [
                    {
                        "secrets": [
                            {
                                "name": i.envar_name,
                                "valueFrom": f"arn:aws:secretsmanager:{AWS_REGION}:{account_id}:secret:{i.secret_name}",
                            }
                            for i in self.secrets
                        ],
                    }
                ],
                "ephemeralStorage": {"sizeInGiB": self.ephemeral_storage_gb},
            },
        )  # type: ignore
        result = ecs_block.save(block_name, overwrite=True)
        LOGGER.info(f"ECS Task Block {block_name} has been pushed with id {result}...")
        return block_name

    def push_deployments(self, flow_path: Path):
        """Method for processing the flows deployments.

        Args:
            project_name (str): Project name that get passed in from the FlowSpec
            so that we can leverage the project's filesystem.
            flow_path (Path): Path object for file name pf  flow file.
        """
        project_name = self._project_name
        ecs_block_name = self._create_ecs_task_block()
        s3_block_name = f"{project_name}-{ENVIRONMENT_TYPE}-{DEPLOYMENT_TYPE}"
        s3 = S3.load(s3_block_name)
        LOGGER.info(f"Loaded S3 Block: {s3_block_name} for S3 Path: {s3.bucket_path}...")  # type: ignore
        ecs = ECSTask.load(ecs_block_name)
        LOGGER.info(f"Using ECS Block: {ecs.name}...")  # type: ignore
        skip_upload = False
        flow_folder = get_flow_folder(flow_path)
        for d in self.deployments:
            d.push_deployment(
                project_name,
                f"{s3_block_name}/{flow_folder}/{self._slugified_flow_name}",
                ecs_block_name,  # type: ignore
                flow_path,
                self.flow.fn.__name__,
                skip_upload,
            )
            skip_upload = True


class PrefectProject(BaseModel):
    """Model used for fetching the project configuration and deploying docker envs and flows as needed."""

    _project_name: str = PrivateAttr()
    _project_docker_envs: list[FlowDockerEnv] = PrivateAttr()
    _prefect_flows_folder: DirectoryPath = PrivateAttr()

    def __init__(self, **data) -> None:
        """Set the private fields on intansiation."""
        super().__init__(**data)
        with open("pyproject.toml") as t:
            config = toml.load(t)
        project_name = standard_slugify(config["tool"]["poetry"]["name"])
        self._project_name = project_name
        self._project_docker_envs = []
        envs = self._project_docker_envs
        for k, v in config["tool"].get("prefect", {"envs": {}})["envs"].items():
            env = FlowDockerEnv(project_name=project_name, env_name=k, **v)
            envs.append(env)
        self._prefect_flows_folder = (
            config["tool"].get("prefect", {}).get("flows_folder", "src")
        )

    def process_project_docker_envs(self, build_only: bool = False) -> None:
        """Method to process project docker envs from the project's
        pyproject.toml

        Args:
            build_only (bool, optional): Whether to run docker build only. Defaults to False.
        """
        project_name = self._project_name
        envs = self._project_docker_envs
        project_docker_envs = ProjectEnvs(project_name=project_name, docker_envs=envs)
        project_docker_envs.process(build_only)

    def process_project_flows(self, validate_only: bool = False) -> None:
        """Method to process project flows using the path from the project's
        pyproject.toml

        Args:
            validate_only (bool, optional): If set to True, we will only validate the FLOW_SPEC model. Defaults to False.

        Raises:
            Exception: Exception will thrown for bad FLOW_SPEC definitions.
        """
        flows_path = self._prefect_flows_folder
        flow_errors = []
        missing_flow_specs = []
        if not validate_only:
            command_type = "deployment"
        else:
            command_type = "validation"
        for name in glob.glob(os.path.join(flows_path, "**/*_flow.py"), recursive=True):
            path_object = Path(name)
            file_name = path_object.name
            mod = file_name.split(".")[0]
            LOGGER.info(f"Running {command_type} for flow: {mod} at path: {name}...")
            try:
                x = SourceFileLoader(mod, name).load_module().FLOW_SPEC
                if not validate_only:
                    x.push_deployments(path_object)
            except AttributeError as e:
                LOGGER.info(e)
                LOGGER.info(f"Flow {mod} does not have a FLOW_SPEC defined!")
                missing_flow_specs.append(mod)
            except ValidationError as e:
                LOGGER.error(e)
                LOGGER.error(f"FLOW_SPEC for {mod} is not properly defined!")
                flow_errors.append(mod)
            except Exception as e:
                LOGGER.error(e)
                flow_errors.append(mod)
        if missing_flow_specs:
            LOGGER.info(
                f"The following flows are missing FLOW_SPEC defintions: {missing_flow_specs}"
            )
            LOGGER.info("This is assumed as expected and will not throw an Exception.")
        if flow_errors:
            raise Exception(f"The following flows failed {command_type}: {flow_errors}")
        LOGGER.info(
            f"All flows with proper FLOW_SPEC definitions have completed {command_type} successfully!"
        )
