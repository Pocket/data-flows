"""Deployment CLI for executing CI/CD workflows"""
import logging
import sys
from argparse import ArgumentParser, Namespace, RawTextHelpFormatter

from common.deployment import LOGGER_NAME, PrefectProject

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


def parse_args(args: list[str]) -> Namespace:
    """Basic CLI setup for running deployment utils as needed.

    Args:
        args (list[str]): List of args passed from cli execution.

    Returns:
       Namespace: parsed args object
    """
    parser = ArgumentParser(
        description="""basic CLI setup for running deployment utils as needed.
    """,
        formatter_class=RawTextHelpFormatter,
    )
    subparsers = parser.add_subparsers(
        title="subcommands",
        help="these are the subcommands to use for deploying flows and environments as needed",
        metavar="",
        dest="subparser_name",
    )
    deploy_envs = subparsers.add_parser(
        "deploy-envs",
        help="""use this command to deploy the docker envs configured in your pyproject.toml
this will also deploy your project's filesystem.
        """,
    )
    deploy_envs.add_argument(
        "--build-only",
        action="store_true",
        help="set this flag to only run Docker build.",
    )
    deploy_flows = subparsers.add_parser(
        "deploy-flows",
        help="use this command to deploy flows from the flows_folder configured in your pyproject.toml",
    )
    deploy_flows.add_argument(
        "--validate-only",
        action="store_true",
        help="set this flag to only validate flow specs.",
    )
    return parser.parse_args(args)


def main() -> None:
    """Main function for operationalizing CLI"""
    p = PrefectProject()
    parsed = parse_args(sys.argv[1:])
    if parsed.subparser_name == "deploy-envs":
        p.process_project_docker_envs(parsed.build_only)
    elif parsed.subparser_name == "deploy-flows":
        p.process_project_flows(parsed.validate_only)
