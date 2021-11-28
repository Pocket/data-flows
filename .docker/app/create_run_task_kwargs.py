#!/usr/bin/env python
import argparse
import logging
from os import environ
from typing import Dict, List

import yaml


def main(args):
    """ Main entry point of the app """
    run_task_kwargs = get_run_task_kwargs()
    logging.info(f"run_task_kwargs = {run_task_kwargs}")

    with open(args.output_path, 'w') as fp:
        yaml.dump(run_task_kwargs, fp)
        logging.info(f"Done. Created {args.output_path}")


def get_run_task_kwargs() -> Dict:
    """
    Get configuration for Prefect's --run-task-kwargs argument, to control how tasks are started by Prefect in ECS.
    @see https://docs.prefect.io/orchestration/agents/ecs.html#custom-runtime-options
    @see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task
    :return: run_task_kwargs as Python dictionary
    """
    return {
        "networkConfiguration": {
            "awsvpcConfiguration": {
                "subnets": get_subnets(),
                "securityGroups": get_security_groups(),
                "assignPublicIp": "DISABLED"
            }
        }
    }


def get_subnets():
    """
    Gets subnets from a comma-separated environment variable
    :return: List of subnets
    """
    return __get_comma_separated_environment_variable('RUN_TASK_SUBNETS', [])


def get_security_groups() -> List[str]:
    """
    Gets security groups from a comma-separated environment variable
    :return: List of security groups
    """
    return __get_comma_separated_environment_variable('RUN_TASK_SECURITY_GROUPS', [])


def __get_comma_separated_environment_variable(name: str, default: List) -> List[str]:
    value = environ.get(name)
    return value.split(',') if value else default


if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    parser.add_argument("-o", "--output-path", type=str, required=True, help="Output Yaml file path")

    parser.add_argument(
        '-v', '--verbose',
        help="Be verbose",
        action="store_const", dest="loglevel", const=logging.INFO,
    )

    args = parser.parse_args()
    # Configure logging
    logging.basicConfig(
        level=args.loglevel,
        format='%(levelname)s [%(filename)s:%(funcName)s] %(message)s',
    )

    main(args)
