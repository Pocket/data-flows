import inspect
import os
from pathlib import Path


def get_script_path():
    frame = inspect.stack()[1]
    filename = frame[0].f_code.co_filename
    return os.path.dirname(os.path.realpath(filename))


def find_pyproject_file(search_folder: str | None = None) -> str:
    """Helper method to look backwards for pyproject.toml

    Args:
        search_folder (str | None, optional): Folder to start search from.
        Defaults to None.

    Returns:
        str: Absolute path of pyproject.toml
    """
    if not search_folder:
        search_folder = os.getcwd()
    sf_path = Path(search_folder)
    files = list(sf_path.glob("pyproject.toml"))
    if files:
        return str(files[0])
    else:
        return find_pyproject_file(str(sf_path.parents[0]))
