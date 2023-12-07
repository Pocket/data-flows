"""Deployment CLI for executing CI/CD workflows"""
import logging
import sys
from asyncio import run
from enum import Enum
from typing import Annotated

import typer
from common.deployment.worker import (
    LOGGER_NAME,
    SCRIPT_PATH,
    get_pyproject_metadata,
    run_command,
)
from common.deployment.worker.check_version import main as cv

# Get deployment logger and setup logging config
CLI_LOGGER = logging.getLogger(LOGGER_NAME)
logging.getLogger("prefect")
CLI_LOGGER.setLevel("DEBUG")
CLI_LOGGER.propagate = False
log_format = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] [%(name)s] [%(funcName)s():%(lineno)s] %(message)s"
)
log_stream = logging.StreamHandler(sys.stdout)
log_stream.setFormatter(log_format)
CLI_LOGGER.addHandler(log_stream)

app = typer.Typer()


# adding per Typer docs
@app.callback()
def callback():
    """CLI for managing deployments and helping with development."""
    pass


# will eventually add gcp and potentially others
class PushTypes(str, Enum):
    aws = "aws"


@app.command()
def process_docker_envs(
    build_only: Annotated[bool, typer.Option("--build-only")] = False,
    push_type: Annotated[PushTypes, typer.Option()] = "aws",  # type: ignore
):
    """CLI command for processing the docker environments.

    Args:
        build_only (Annotated[bool, typer.Option, optional):
        Whether to only run docker build process. Defaults to False.
        push_type (Annotated[PushTypes, typer.Option, optional):
        Where docker environments are to be pushed. Defaults to "aws".
    """
    run_command(f"{SCRIPT_PATH}/common_utils_build.sh")
    p = get_pyproject_metadata()
    p.process_docker_envs(build_only, push_type)


@app.command()
def process_flow_specs():
    """CLI command to process the flow specs and create prefect.yaml."""
    p = get_pyproject_metadata()
    run(p.process_flow_specs())


@app.command()
def check_version():
    """CLI command to check for proper version bump."""
    cv()


@app.command()
def clone_project(gh_repo: str, branch: str, root_dir: str = "opt"):
    """CLI command to use for cloning project in monorepo.

    Args:
        gh_repo (str): Github report where project lives.
        branch (str): Branch to clone
        root_dir (str, optional): Root directory for temporary clone. Defaults to "opt".
    """
    p = get_pyproject_metadata()
    p.clone_project(gh_repo, branch, root_dir)


@app.command()
def build_common_utils():
    """CLI command to build common utils for Docker build process."""
    run_command(f"{SCRIPT_PATH}/common_utils_build.sh")


if __name__ == "__main__":
    app()
