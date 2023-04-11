import os
from pathlib import Path

from common.deployment import LOGGER, run_command


def get_poetry_version():
    version_data = run_command("poetry version")
    return version_data.strip().split()


def get_main_version_tag():
    try:
        tag_data = run_command(
            f"""
            set -e
            rm -rf /tmp/data-flows-clone
            git clone ../ /tmp/data-flows-clone
            cd /tmp/data-flows-clone/{Path(os.getcwd()).name}
            echo $PWD
            git checkout main-v2
            poetry version
            """
        )
        return tag_data.strip().split()
    except Exception as e:
        if "Poetry could not find a pyproject.toml file" in str(e):
            LOGGER.info("no existing project...")
            LOGGER.info("returning 0.0.0 as version...")
            return "new-project 0.0.0".strip().split()
        else:
            raise Exception(str(e))
    finally:
        run_command("rm -rf /tmp/data-flows-clone")


def version_compare(v1, v2):

    # This will split both the versions by '.'
    arr1 = v1.split(".")
    arr2 = v2.split(".")
    n = len(arr1)
    m = len(arr2)

    # converts to integer from string
    arr1 = [int(i) for i in arr1]
    arr2 = [int(i) for i in arr2]

    # compares which list is bigger and fills
    # smaller list with zero (for unequal delimeters)
    if n > m:
        for i in range(m, n):
            arr2.append(0)
    elif m > n:
        for i in range(n, m):
            arr1.append(0)

    # returns 1 if version 1 is bigger and -1 if
    # version 2 is bigger and 0 if equal
    for i in range(len(arr1)):
        if arr1[i] > arr2[i]:
            return True
        elif arr2[i] > arr1[i]:
            return False
    return False


def main():
    new_version = tuple(get_poetry_version())[1]
    current_version = tuple(get_main_version_tag())[1]
    v1 = new_version
    v2 = current_version
    if not version_compare(v1, v2):
        raise Exception(f"{v1} is not bigger than {v2}, please bump version.")
    else:
        LOGGER.info(f"{v1} is bigger than {v2}...version check passed.")
