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


@app.callback()
def callback():
    """CLI for managing deployments and helping with development."""
    pass


class PushTypes(str, Enum):
    aws = "aws"


@app.command()
def process_docker_envs(
    build_only: Annotated[bool, typer.Option("--build-only")] = False,
    push_type: Annotated[PushTypes, typer.Option()] = "aws",  # type: ignore
):
    p = get_pyproject_metadata()
    p.process_docker_envs(build_only, push_type)


@app.command()
def process_flow_specs():
    p = get_pyproject_metadata()
    run(p.process_flow_specs())


@app.command()
def check_version():
    cv()


@app.command()
def clone_project(gh_repo: str, branch: str, root_dir: str = "opt"):
    p = get_pyproject_metadata()
    p.clone_project(gh_repo, branch, root_dir)


@app.command()
def build_common_utils():
    run_command(f"{SCRIPT_PATH}/common_utils_build.sh")


if __name__ == "__main__":
    app()
