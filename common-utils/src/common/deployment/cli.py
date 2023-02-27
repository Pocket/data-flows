"""Deployment CLI for executing CI/CD workflows"""
import logging
import sys
from argparse import ArgumentParser, RawTextHelpFormatter, Namespace

from common.deployment import LOGGER_NAME, process_project_env

# Get deployment logger and setup logging config
CLI_LOGGER = logging.getLogger(LOGGER_NAME)
CLI_LOGGER.setLevel("INFO")
CLI_LOGGER.propagate = False
log_format = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] [%(name)s] [%(funcName)s():%(lineno)s] %(message)s"
)
log_stream = logging.StreamHandler(sys.stdout)
log_stream.setFormatter(log_format)
CLI_LOGGER.addHandler(log_stream)


def parse_args(args: list[str]) -> Namespace:
    """Basic CLI setup for running deploymebt utils as needed.

    Args:
        args (_type_): _description_

    Returns:
        _type_: _description_
    """
    """Basic CLI for running deploymebt utils as needed."""
    parser = ArgumentParser(
        description="""Running this cli directly will:
    - Build and push your Docker images to our private ECR repository.
    - Create an S3 filesytem block in Prefect v2 for storing flows and
    for storing flow run staging/working files.
    """,
        formatter_class=RawTextHelpFormatter,
    )
    parser.add_argument(
        "--build-only",
        action="store_true",
        help="Set this flag to only run Docker build.",
    )
    return parser.parse_args(args)


def main(args: list[str] = []) -> None:
    """Main function for operationalizing CLI"""
    parsed = parse_args(args)
    process_project_env(parsed.build_only)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
