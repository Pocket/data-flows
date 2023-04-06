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
    conint,
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
AWS_SUBNETS = os.getenv("AWS_SUBNETS", "[]")
AWS_SECURITY_GROUPS = os.getenv("AWS_SECURITY_GROUPS", "[]")

# config for supporting using different pyproject.toml path when developing flows
# running deployment cli with this set may result in errors
# this is actually used in the flow deployment run_command script, because the prefect cli needs to run from the directory of the flow files
CWD_DIR = os.path.join(os.getcwd(), "pyproject.toml")

PYPROJECT_PATH = os.path.abspath(
    os.path.expanduser(os.getenv("PREFECT_PYPROJECT_PATH", CWD_DIR))
)

# config to disable flow spec processing logic
# we do not want to run this during flow execution
DISABLE_FLOW_SPEC = eval(os.getenv("PREFECT_DISABLE_FLOW_SPEC", "False"))


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


# model for pyproject metadata
class PyProjectMetadata(BaseModel):
    """Model to call namespaced metadata"""

    project_name: str
    prefect_flows_folder: str
    docker_envs: dict


# creating and running a helper function to extract project metadata.
def get_pyproject_metadata() -> PyProjectMetadata:
    """Produce a PyProjectMetadata object from PYPROJECT_PATH.
    This means we do not want to rely on existence of this file
    for flow executions.

    Returns:
        PyProjectMetadata: Model containing the pyproject metadata.
    """
    with open(PYPROJECT_PATH) as t:
        config = toml.load(t)
    project_name = standard_slugify(config["tool"]["poetry"]["name"])
    prefect_flows_folder = config["tool"].get("prefect", {}).get("flows_folder", "src")
    docker_envs = config["tool"].get("prefect", {"envs": {}})["envs"]
    return PyProjectMetadata(
        project_name=project_name,
        prefect_flows_folder=prefect_flows_folder,
        docker_envs=docker_envs,
    )


def create_image_name_str(project_name: str, env_name: str, python_version: str) -> str:
    """Helper function to create image name to enable resuability.

    Args:
        env_name (str): Slugified Docker env name.
        python_version (str): Python version.

    Returns:
        str: _description_
    """
    return f"{project_name}-{standard_slugify(env_name)}-py-{python_version}"


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
            self.project_name, self.env_name, self.python_version  # type: ignore
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

    docker_envs: list[FlowDockerEnv]

    def _push_environments(self, build_only: bool = False) -> None:
        """Build and optionally push ECR Image.

        Args:
            build_only (bool, optional): _description_. Defaults to False.
        """
        account_id = get_aws_account_id()
        for env in self.docker_envs:
            env.build_image()
            if not build_only:
                env.push_image(account_id)

    def _create_file_systems(self, build_only: bool = False) -> None:
        """Create Prefect S3 filesystem for project.

        Args:
            build_only (bool, optional): Switch to only run if True. Defaults to False.
        """
        project_metadata = get_pyproject_metadata()
        if not build_only:
            project_name = project_metadata.project_name
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
            return f"--cron '{schedule.cron}'"
        elif isinstance(schedule, RRuleSchedule):
            return f"--rrule '{schedule.rrule}'"
        elif isinstance(schedule, IntervalSchedule):
            return f"--interval '{schedule.interval.seconds}'"
        else:
            return ""

    def push_deployment(
        self,
        storage_path: str,
        infrastructure: str,
        flow_path: Path,
        flow_function_name: str,
        skip_upload: bool,
    ) -> None:
        """This method will push a Prefect deployment to Prefect using the deployment cli.

        Args:
            storage_block_path (str): Remote storage block path to use for flow files.
            infrastructure (str): ECS Task block name to use for the flow execution environment.
            flow_path (Path): Relative path to the flow python file.
            flow_function_name: (str): Name of the flow function to deploy.
            skip_upload (bool): Whether to skip upload. Will be set to True after first deployment pushed for flow.
        """
        pyproject_metadata = get_pyproject_metadata()
        project_name = pyproject_metadata.project_name
        deployment_name = standard_slugify(self.deployment_name)
        flow_file_name = flow_path.name
        task_customizations = [
            {
                "op": "add",
                "path": "/overrides/cpu",
                "value": self.cpu,
            },
            {
                "op": "add",
                "path": "/overrides/memory",
                "value": self.memory,
            },
            {
                "op": "add",
                "path": "/networkConfiguration/awsvpcConfiguration/subnets",
                "value": json.loads(AWS_SUBNETS),
            },
            {
                "op": "add",
                "path": "/networkConfiguration/awsvpcConfiguration/securityGroups",
                "value": json.loads(AWS_SECURITY_GROUPS),
            },
            {
                "op": "add",
                "path": "/networkConfiguration/awsvpcConfiguration/assignPublicIp",
                "value": "DISABLED",
            },
        ]
        overrides = (
            f"""--override task_customizations='{json.dumps(task_customizations)}'"""
        )
        schedule = self._get_schedule_arg()
        params = f"'{json.dumps(self.parameters)}'"
        skip_upload_flag = ""
        if skip_upload:
            skip_upload_flag = "--skip-upload"
        # We are using the escape hatch envar PREFECT_PYPROJECT_PATH because of limitation in prefect CLI
        # We may remove this in the future.
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
        description="docker environment name from pyproject.toml, which is the key name in '[tool.prefect.envs.<>]'",
    )
    ephemeral_storage_gb: conint(gt=19, lt=201) = 20  # type: ignore
    deployments: list[FlowDeployment] = []
    _slugified_flow_name: StrictStr = PrivateAttr()
    _slugified_fn_name: StrictStr = PrivateAttr()
    _project_name: StrictStr = PrivateAttr()
    _task_definition_arn: StrictStr = PrivateAttr()

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data) -> None:
        """Set the private fields on intansiation.
        DISABLE_FLOW_SPEC will be set to true to disable validation for ECS execution.
        """
        if not DISABLE_FLOW_SPEC:
            super().__init__(**data)
            pyproject_metadata = get_pyproject_metadata()
            project_name = pyproject_metadata.project_name
            slugified_fn_name = standard_slugify(self.flow.fn.__name__)
            self._project_name = project_name
            self._slugified_fn_name = slugified_fn_name
            self.flow.name = f"{project_name}.{slugified_fn_name}"
            self._slugified_flow_name = standard_slugify(self.flow.name)

    @validator("docker_env")
    def docker_env_must_be_registered(cls, v):
        pyproject_metadata = get_pyproject_metadata()
        docker_envs = pyproject_metadata.docker_envs
        project_name = pyproject_metadata.project_name
        if v not in docker_envs.keys():
            raise ValueError(
                "Docker env does not exist in pyproject.toml.  It must be the key name in a '[tool.prefect.envs.<key name>]' configuration."
            )
        else:
            # return image string
            return create_image_name_str(project_name, v, docker_envs[v]["python_version"])  # type: ignore

    def _handle_task_definition(self):
        register = False
        final_task_def_arn = None
        task_name = f"{self._slugified_flow_name}-{DEPLOYMENT_TYPE}"
        account_id = get_aws_account_id()
        init_session = session.Session()
        ecs = init_session.client("ecs")
        image_name = f"{account_id}.dkr.ecr.{AWS_REGION}.amazonaws.com/data-flows-prefect-envs:{self.docker_env}-{GIT_SHA}"
        task_role_arn = f"arn:aws:iam::{account_id}:role/data-flows-prefect-{DEPLOYMENT_TYPE}-task-role"
        execution_role_arn = f"arn:aws:iam::{account_id}:role/data-flows-prefect-{DEPLOYMENT_TYPE}-exec-role"
        secrets = [
            {
                "name": i.envar_name,
                "valueFrom": f"arn:aws:secretsmanager:{AWS_REGION}:{account_id}:secret:{i.secret_name}",
            }
            for i in self.secrets
        ]
        ephemeral_storage = self.ephemeral_storage_gb
        default_cpu = "256"
        default_memory = "512"

        task_def_compare_keys = [
            "taskRoleArn",
            "executionRoleArn",
            "requiresCompatibilities",
            "cpu",
            "memory",
            "ephemeralStorage",
        ]

        container_def_compare_keys = [
            "image",
            "environment",
            "secrets",
            "logConfiguration",
        ]

        default_container_name = "prefect"

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
                        {"name": "PREFECT_DISABLE_FLOW_SPEC", "value": "True"},
                    ],
                    "secrets": secrets,
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-create-group": "true",
                            "awslogs-group": default_container_name,
                            "awslogs-region": AWS_REGION,
                            "awslogs-stream-prefix": task_name,
                        },
                    },
                }
            ],
            "requiresCompatibilities": [
                "FARGATE",
            ],
            "cpu": default_cpu,
            "memory": default_memory,
            "ephemeralStorage": {"sizeInGiB": ephemeral_storage},
        }

        task_def_arns = ecs.list_task_definitions(familyPrefix=task_name, maxResults=5)
        if task_def_arns["taskDefinitionArns"]:
            current_task_def_response = ecs.describe_task_definition(
                taskDefinition=task_name
            )
            current_task_def = current_task_def_response["taskDefinition"]
            final_task_def_arn = current_task_def["taskDefinitionArn"]
            if not all(
                new_task_def_dict.get(key) == current_task_def.get(key)
                for key in task_def_compare_keys
            ):
                LOGGER.info(
                    f"Task definition state has changed for {task_name}, registering new definition..."
                )
                register = True
            elif not all(
                new_task_def_dict.get("containerDefinitions", [{}])[0].get(key)
                == current_task_def.get("containerDefinitions", [{}])[0].get(key)
                for key in container_def_compare_keys
            ):
                LOGGER.info(
                    f"Task definition state has changed for {task_name}, registering new definition..."
                )
                register = True

        else:
            LOGGER.info(
                f"Task definition not registered for {task_name}, registering new definition..."
            )
            register = True

        if register:
            response = ecs.register_task_definition(**new_task_def_dict)
            final_task_def_arn = response["taskDefinition"]["taskDefinitionArn"]
        else:
            LOGGER.info(
                f"Task definition state has not changed {task_name}, new revision not needed..."
            )
        return final_task_def_arn

    def _create_ecs_task_block(self):
        """This method will create the ECS Task block as supported by common-utils."""
        block_name = f"{self._slugified_flow_name}-{ENVIRONMENT_TYPE}-{DEPLOYMENT_TYPE}"
        ecs_block = ECSTask(
            name=block_name,
            task_definition_arn=self._handle_task_definition(),
            cluster=f"prefect-v2-agent-{ENVIRONMENT_TYPE}-{DEPLOYMENT_TYPE}",
            launch_type="FARGATE",
            allow_task_definition_registration=False,
        )  # type: ignore
        result = ecs_block.save(block_name, overwrite=True)
        LOGGER.info(f"ECS Task Block {block_name} has been pushed with id {result}...")
        return block_name

    def push_deployments(self, flow_path: Path):
        """Method for processing the flows deployments.

        Args:
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
                f"{s3_block_name}/{flow_folder}/{self._slugified_fn_name}",
                ecs_block_name,  # type: ignore
                flow_path,
                self.flow.fn.__name__,
                skip_upload,
            )
            skip_upload = True


class PrefectProject(BaseModel):
    """Model used for fetching the project configuration and deploying docker envs and flows as needed."""

    _pyproject_metadata: PyProjectMetadata = PrivateAttr()
    _project_docker_envs: list[FlowDockerEnv] = PrivateAttr()
    _prefect_flows_folder: DirectoryPath = PrivateAttr()

    def __init__(self, **data) -> None:
        """Set the private fields on intansiation."""
        super().__init__(**data)
        self._pyproject_metadata = get_pyproject_metadata()
        pyproject_metadata = self._pyproject_metadata
        project_name = pyproject_metadata.project_name
        self._project_docker_envs = []
        envs = self._project_docker_envs
        for k, v in pyproject_metadata.docker_envs.items():
            env = FlowDockerEnv(env_name=k, project_name=project_name, **v)
            envs.append(env)
        self._prefect_flows_folder = Path(pyproject_metadata.prefect_flows_folder)

    def process_project_docker_envs(self, build_only: bool = False) -> None:
        """Method to process project docker envs from the project's
        pyproject.toml

        Args:
            build_only (bool, optional): Whether to run docker build only. Defaults to False.
        """
        envs = self._project_docker_envs
        project_docker_envs = ProjectEnvs(docker_envs=envs)
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
