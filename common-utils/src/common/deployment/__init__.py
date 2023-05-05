"""Deployment utilities module with tools for Prefect CI/CD"""
import glob
import json
import logging
import os
import shlex
from importlib.machinery import SourceFileLoader
from pathlib import Path
from subprocess import PIPE, STDOUT, Popen
from typing import Any, Literal

import toml
from prefect import Flow
from prefect.server.schemas.schedules import (
    SCHEDULE_TYPES,
    CronSchedule,
    IntervalSchedule,
    RRuleSchedule,
)
from pydantic import (
    BaseModel,
    DirectoryPath,
    Field,
    FilePath,
    PrivateAttr,
    StrictStr,
    conint,
)
from pydantic.error_wrappers import ValidationError
from slugify import slugify

# Create logger for module
LOGGER_NAME = __name__
LOGGER = logging.getLogger(LOGGER_NAME)

# script path for referencing bash scripts
SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))

# config for deploying task definitions and flows
DEPLOYMENT_TYPE = os.getenv("POCKET_PREFECT_DEPLOYMENT_TYPE", "dev").lower()
GIT_SHA = os.getenv("CIRCLE_SHA1", "dev")[0:7]
AWS_REGION = os.getenv("DEFAULT_AWS_REGION", "us-east-1").lower()
AWS_SUBNETS = os.getenv("POCKET_AWS_SUBNETS", "[]")
AWS_SECURITY_GROUPS = os.getenv("POCKET_AWS_SECURITY_GROUPS", "[]")
AWS_VPC_ID = os.getenv("POCKET_AWS_VPC_ID")
PROJECT_ROOT = Path(os.getcwd()).parts[-1]
PYPROJECT_FILE_PATH = os.path.expanduser(os.path.join(os.getcwd(), "pyproject.toml"))


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
    """Produce a PyProjectMetadata object from the pyproject.toml file.

    Returns:
        PyProjectMetadata: Model containing the pyproject metadata.
    """
    with open(PYPROJECT_FILE_PATH) as t:
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


def get_aws_account_id(session: object) -> str:
    """Helper function to get current AWS account id
    using session object.

    Returns:
        str: Accout id as string.
    """
    sts = session.client("sts")  # type: ignore
    return sts.get_caller_identity()["Account"]


def get_flow_folder(flow_path: Path) -> str:
    """Helper function to create a slugified string from the flow folder.
    Data flows are meant to be a Python file at the root of a 'flow folder'.
    We use this slugified name for naming flows in Prefect.

    Args:
        flow_path (Path): Path object for relative path of Python flow file.

    Returns:
        str: Slugified string from flow folder.
    """
    return standard_slugify(flow_path.parts[-2:-1][0])


class FlowDockerEnv(BaseModel):
    """Model that represents a Prefect Docker environment."""

    env_name: StrictStr
    dockerfile_path: FilePath
    python_version: Literal["3.10"]
    docker_build_context: DirectoryPath = Path(".")
    project_name: StrictStr
    _image_name: str = PrivateAttr()

    def build_image(self) -> None:
        """Build a docker image using model attributes."""
        # get proper image name and set instance private attr
        image_name = create_image_name_str(
            self.project_name, self.env_name, self.python_version
        )
        self._image_name = image_name
        # build more args and run image build
        dockerfile_path = self.dockerfile_path
        python_version = self.python_version
        docker_build_context = self.docker_build_context
        run_command(
            shlex.join(
                [
                    f"{SCRIPT_PATH}/build_image.sh",
                    image_name,
                    str(dockerfile_path),
                    python_version,
                    str(docker_build_context),
                ]
            )
        )

    def push_image(self) -> str:
        """Push built image to ECR

        Returns:
            str: Pushed ECR image name.
        """
        # only need AWS SDK on deployment
        from boto3 import session

        init_session = session.Session()
        account_id = get_aws_account_id(init_session)
        image_name = self._image_name
        # push image to ECR using stored image name and AWS account id
        pushed_image = run_command(
            shlex.join([f"{SCRIPT_PATH}/push_image.sh", image_name, account_id])
        )
        return pushed_image


class ProjectEnvs(BaseModel):
    """Model that represents the project environment.
    This will store the list of docker envs to build and push.
    The pyproject.toml configuration is used here.
    """

    docker_envs: list[FlowDockerEnv]
    pyproject_metadata: PyProjectMetadata

    def _push_environments(self, build_only: bool = False) -> None:
        """Build and optionally push ECR Image.

        Args:
            build_only (bool, optional): _description_. Defaults to False.
        """
        for env in self.docker_envs:
            env.build_image()
            if not build_only:
                env.push_image()

    def process(self, build_only: bool = False) -> None:
        """Method to run project envs workflow

        Args:
            build_only (bool, optional): Swtich to pass to methods. Defaults to False.
        """
        self._push_environments(build_only)


class FlowEnvar(BaseModel):
    """Model used to define an ECS Task envar that
    either sources from Secrets Manager or is plain text.
    If a secret, the value must be only the secret name and not the full ARN.
    The deployment process will create the ARN for you.

    Plain text would be a create use case for "EXTRA_PIP_PACKAGES"
    described here: https://docs.prefect.io/latest/concepts/infrastructure/#installing-extra-dependencies-at-runtime.
    """

    envar_name: StrictStr
    envar_value: StrictStr


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
    schedule: SCHEDULE_TYPES = (Field(None, description="Schedule configuration for this deployment using Prefect Schedule Objects."),)  # type: ignore
    envars: list[FlowEnvar] = Field(
        [],
        description="List of plain text environment variables to pass to task run overrides",
    )

    def _get_schedule_arg(self):
        """Helper function to translate Prefect Schedule Object into a command line argument.

        Returns:
            str: Command line argument to use for setting the deployment's schedule.
        """
        schedule = self.schedule
        # we should only schedule for main flows
        if not DEPLOYMENT_TYPE == "main":
            return ""
        else:
            if isinstance(schedule, CronSchedule):
                return shlex.join(["--cron", schedule.cron])
            elif isinstance(schedule, RRuleSchedule):
                return shlex.join(["--rrule", schedule.rrule])
            elif isinstance(schedule, IntervalSchedule):
                return shlex.join(["--interval", str(schedule.interval.seconds)])
            else:
                return ""

    def push_deployment(
        self,
        project_name: str,
        infrastructure: str,
        flow_path: Path,
        flow_function_name: str,
        flow_name: str,
    ) -> None:
        """This method will push a Prefect deployment to Prefect using the deployment cli.

        Args:
            storage_block_path (str): Remote storage block path to use for flow files.
            infrastructure (str): ECS Task block name to use for the flow execution environment.
            flow_path (Path): Relative path to the flow python file.
            flow_function_name: (str): Name of the flow function to deploy.
            flow_name: (str): Properly enforced flow name for deployment.

        """
        # start building the cli arguments
        deployment_name = f"{standard_slugify(self.deployment_name)}-{DEPLOYMENT_TYPE}"
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
        env_overrides = " ".join(
            [
                "--override "
                + shlex.quote(
                    f"env.{i.dict()['envar_name']}='{i.dict()['envar_value']}'"
                )
                for i in self.envars
            ]
        )
        task_overrides = (
            f"""--override task_customizations='{json.dumps(task_customizations)}'"""
        )
        overrides = env_overrides + " " + task_overrides
        schedule = self._get_schedule_arg()
        # envars here are used for local testing purposes
        infra_arg = os.getenv(
            "POCKET_PREFECT_INFRASTRUCTURE_BLOCK", f"-ib ecs-task/{infrastructure}"
        )
        github_block = os.getenv(
            "POCKET_PREFECT_GITHUB_BLOCK", f"data-flows-{DEPLOYMENT_TYPE}"
        )
        project_root_flow_path = f"{PROJECT_ROOT}/{flow_path.parent}"
        # run deployment cli using helper function
        run_command(
            f"""export POCKET_PREFECT_FLOW_NAME={shlex.quote(flow_name)} && \\
        pushd {shlex.quote("../")} && \\
        prefect deployment build {shlex.quote(f'{project_root_flow_path}/{flow_file_name}:{flow_function_name}')} \\
        -n {shlex.quote(deployment_name)} \\
        -sb {shlex.quote(f'github/{github_block}/{project_root_flow_path}')} \\
        {shlex.join(shlex.split(infra_arg))} \\
        {shlex.join(shlex.split(overrides))} \\
        -q {shlex.quote('prefect-v2-queue-' + DEPLOYMENT_TYPE)} \\
        -v {shlex.quote(GIT_SHA)} \\
        {shlex.join(['--params', json.dumps(self.parameters)])} \\
        -t {shlex.quote(project_name)} -t {shlex.quote(get_flow_folder(flow_path))} -t {shlex.quote(DEPLOYMENT_TYPE)} \\
        -a \\
        {schedule} --skip-upload && \\
        popd"""
        )

        LOGGER.info(
            f"Deployment: {deployment_name} for flow: {flow_path} applied successfully..."
        )


class FlowSpec(BaseModel):
    """This is the object we use to deploy a flow."""

    flow: Flow = Field(..., description="Flow funciton object.")
    secrets: list[FlowEnvar] = Field(
        [],
        description=(
            "List of secrets as FlowEnvar objects to pass to task definition."
            "These should only be the secret names and not full arn."
        ),
    )
    docker_env: StrictStr = Field(
        ...,
        description="docker environment name from pyproject.toml, which is the key name in '[tool.prefect.envs.<>]'",
    )
    ephemeral_storage_gb: conint(gt=19, lt=201) = 20  # type: ignore
    deployments: list[FlowDeployment] = []

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data) -> None:
        """Set the flow name on deployment."""
        super().__init__(**data)
        if x := os.getenv("POCKET_PREFECT_FLOW_NAME"):
            self.flow.name = x

    def _handle_task_definition(
        self, account_id: str, ecs_client: object, slugified_flow_name: str
    ) -> str | None:
        """Register Task Defintition with AWS ECS as needed based on changes.

        Args:
            account_id (str): AWS account id
            ecs_client (object): valid ecs client
            slugified_flow_name (str): slugified flow name for naming
            of task definition and block

        Returns:
            str: New or existing Task Definition ARN.
        """
        # set up default return value register flag
        # evaluations will set register to True as needed
        register = False
        final_task_def_arn = None
        # start building the task definition elements from FlowSpec and globals
        # these values plus other defaults will be the basis for task def diff analysis
        task_name = f"{slugified_flow_name}-{DEPLOYMENT_TYPE}"
        image_name = f"{account_id}.dkr.ecr.{AWS_REGION}.amazonaws.com/data-flows-prefect-v2-envs:{self.docker_env}-{GIT_SHA}"
        task_role_arn = f"arn:aws:iam::{account_id}:role/data-flows-prefect-{DEPLOYMENT_TYPE}-task-role"
        execution_role_arn = f"arn:aws:iam::{account_id}:role/data-flows-prefect-{DEPLOYMENT_TYPE}-exec-role"
        secrets = [
            {
                "name": i.envar_name,
                "valueFrom": f"arn:aws:secretsmanager:{AWS_REGION}:{account_id}:secret:{i.envar_value}",
            }
            for i in self.secrets
        ]
        ephemeral_storage = self.ephemeral_storage_gb
        default_cpu = "256"
        default_memory = "512"
        default_container_name = "prefect"

        # set the compare keys
        # these are used to determine if we need a new task definition registered or not
        task_def_compare_keys = [
            "taskRoleArn",
            "executionRoleArn",
            "requiresCompatibilities",
            "cpu",
            "memory",
            "ephemeralStorage",
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
            "ephemeralStorage": {"sizeInGiB": ephemeral_storage},
            "volumes": [
                {
                    "name": "prefect-scratch",
                }
            ],
        }
        # check to see if we have any registered task definitions
        task_def_arns = ecs_client.list_task_definitions(familyPrefix=task_name, maxResults=5)  # type: ignore
        # if we have some then we treat this as an existing task definition and start diff
        if task_def_arns["taskDefinitionArns"]:
            current_task_def_response = ecs_client.describe_task_definition(  # type: ignore
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
                    f"Task definition state has changed for {task_name}, registering new definition..."
                )
                register = True
            # if any of our container definition keys have changed we mark as "register = True"
            elif not all(
                new_task_def_dict.get("containerDefinitions", [{}])[0].get(key)
                == current_task_def.get("containerDefinitions", [{}])[0].get(key)
                for key in container_def_compare_keys
            ):
                LOGGER.info(
                    f"Task definition state has changed for {task_name}, registering new definition..."
                )
                register = True
        # if no registered arns, we treat this a brand new and mark  "register = True" for initial registration
        else:
            LOGGER.info(
                f"Task definition not registered for {task_name}, registering new definition..."
            )
            register = True
        # if "register = True" we register a task definition
        if register:
            response = ecs_client.register_task_definition(**new_task_def_dict)  # type: ignore
            final_task_def_arn = response["taskDefinition"]["taskDefinitionArn"]
        else:
            LOGGER.info(
                f"Task definition state has not changed {task_name}, new revision not needed..."
            )
        # this will return the current or newly registered task definition version arn
        return final_task_def_arn

    def _create_ecs_task_block(
        self, account_id: str, ecs_client: object, slugified_flow_name: str
    ) -> str:
        """This method will create the ECS Task block as supported by common-utils.

        Args:
            account_id (str): AWS account id
            ecs_client (object): valid ecs client
            slugified_flow_name (str): slugified flow name for naming
            of task definition and block

        Returns:
            str: block name for use in deployment
        """
        # we only need prefect_aws when deploying the Block
        from prefect_aws.ecs import ECSTask

        # create Prefect ECSTask Block
        # we are using a predefined task definition
        # also turing off any additional handling of task definition in the ECSTask methods
        block_name = f"{slugified_flow_name}-{DEPLOYMENT_TYPE}"
        ecs_block = ECSTask(
            name=block_name,
            task_definition_arn=self._handle_task_definition(
                account_id, ecs_client, slugified_flow_name
            ),
            cluster=f"prefect-v2-agent-{DEPLOYMENT_TYPE}",
            launch_type="FARGATE",
            allow_task_definition_registration=False,
            vpc_id=AWS_VPC_ID,
        )  # type: ignore
        result = ecs_block.save(block_name, overwrite=True)
        LOGGER.info(f"ECS Task Block {block_name} has been pushed with id {result}...")
        return block_name

    def push_deployments(
        self,
        flow_path: Path,
        pyproject_metadata: PyProjectMetadata,
        account_id: str,
        ecs_client: object,
    ):
        """Method for processing the flows deployments.

        Args:
            so that we can leverage the project's filesystem.
            flow_path (Path): Path object for file name pf  flow file.

        Args:
            flow_path (Path):  Path object for file name of flow file.
            pyproject_metadata (PyProjectMetadata): metadata from pyproject.toml
            account_id (str): AWS account id
            ecs_client (object): valid ecs client
        """

        # init private attrs that are needed downstream
        project_name = pyproject_metadata.project_name
        slugified_fn_name = standard_slugify(self.flow.fn.__name__)
        # setting flow name to "<subproject name>/<parent directory>/<flow function name>
        flow_name = f"{project_name}.{get_flow_folder(flow_path)}.{slugified_fn_name}"

        # update docker_env to proper image name
        orig_docker_env = self.docker_env
        docker_envs = pyproject_metadata.docker_envs
        self.docker_env = create_image_name_str(
            project_name,
            orig_docker_env,
            docker_envs[orig_docker_env]["python_version"],
        )

        # process deployments
        # create ECS task definition and Prefect Block
        slugified_flow_name = standard_slugify(flow_name)
        # ignore ECS handling if POCKET_PREFECT_INFRASTRUCTURE_BLOCK envar exists
        if not os.getenv("POCKET_PREFECT_INFRASTRUCTURE_BLOCK"):
            ecs_block_name = self._create_ecs_task_block(
                account_id, ecs_client, slugified_flow_name
            )
        else:
            ecs_block_name = "overridden"
        LOGGER.info(f"Using ECS Block: {ecs_block_name}...")
        # loop through deployments and push
        for d in self.deployments:
            d.push_deployment(
                project_name,
                ecs_block_name,
                flow_path,
                self.flow.fn.__name__,
                flow_name,
            )


class PrefectProject(BaseModel):
    """Model used for fetching the project configuration to deploy docker envs and flows as needed."""

    _pyproject_metadata: PyProjectMetadata = PrivateAttr()
    _project_docker_envs: list[FlowDockerEnv] = PrivateAttr()
    _prefect_flows_folder: DirectoryPath = PrivateAttr()

    def __init__(self, **data) -> None:
        """Set the private fields on intansiation."""
        super().__init__(**data)
        # pull the pyproject.toml data needed for processing
        self._pyproject_metadata = get_pyproject_metadata()
        pyproject_metadata = self._pyproject_metadata
        # create default list for docker envs
        self._project_docker_envs = []
        envs = self._project_docker_envs
        # add validated docker envs to private attr
        for k, v in pyproject_metadata.docker_envs.items():
            env = FlowDockerEnv(
                env_name=k, project_name=self._pyproject_metadata.project_name, **v
            )
            envs.append(env)
        self._prefect_flows_folder = Path(pyproject_metadata.prefect_flows_folder)

    def process_project_docker_envs(self, build_only: bool = False) -> None:
        """Method to process project docker envs from the project's
        pyproject.toml

        Args:
            build_only (bool, optional): Whether to run docker build only. Defaults to False.
        """
        envs = self._project_docker_envs
        # create ProjectEnvs model from private attributes
        project_docker_envs = ProjectEnvs(
            docker_envs=envs, pyproject_metadata=self._pyproject_metadata
        )
        # build and/or push the docker envs
        project_docker_envs.process(build_only)

    def process_project_flows(self, validate_only: bool = False) -> None:
        """Method to process project flows using the path from the project's
        pyproject.toml

        Args:
            validate_only (bool, optional): If set to True, we will only validate the FLOW_SPEC model. Defaults to False.

        Raises:
            Exception: Exception will thrown for bad FLOW_SPEC definitions.
        """
        # account_id and ecs client is only populated on deployment
        # need default values for validation to work and prevent unbound variables
        account_id = ""
        ecs_client = object()
        flows_path = self._prefect_flows_folder
        # setup to aggregate errors for raising at the end as needed
        flow_errors = []
        missing_flow_specs = []
        # this is the deployment path
        if not validate_only:
            # command type used for DRY logging
            command_type = "deployment"
            if not os.getenv("POCKET_PREFECT_INFRASTRUCTURE_BLOCK"):
                # only need AWS for deployment
                # and if POCKET_PREFECT_INFRASTRUCTURE_BLOCK does not exist
                # single account_id and ecs client for entire deployment workflow
                from boto3 import session

                init_session = session.Session()
                account_id = get_aws_account_id(init_session)
                ecs_client = init_session.client("ecs")

        else:
            # set command type to validation to skip deployment
            command_type = "validation"

        # loop through list of all files that end with "_flow.py"
        for name in glob.glob(os.path.join(flows_path, "**/*_flow.py"), recursive=True):
            path_object = Path(name)
            file_name = path_object.name
            mod = file_name.split(".")[0]
            LOGGER.info(f"Running {command_type} for flow: {mod} at path: {name}...")
            try:
                # load FLOW_SPEC global from file
                x = SourceFileLoader(mod, name).load_module().FLOW_SPEC
                # validate docker env is registered in pyproject.toml
                if x.docker_env not in self._pyproject_metadata.docker_envs.keys():
                    raise ValueError(
                        "Docker env does not exist in pyproject.toml.  It must be the key name in a '[tool.prefect.envs.<key name>]' configuration."
                    )
                # only run deployment when validate_only is False
                if not validate_only:
                    x.push_deployments(
                        path_object, self._pyproject_metadata, account_id, ecs_client
                    )
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
                f"The following flows are missing FLOW_SPEC defintions: {missing_flow_specs}"
            )
            LOGGER.info("This is assumed as expected and will not throw an Exception.")
        if flow_errors:
            raise Exception(f"The following flows failed {command_type}: {flow_errors}")
        LOGGER.info(
            f"All flows with proper FLOW_SPEC definitions have completed {command_type} successfully!"
        )
