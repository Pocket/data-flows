import inspect
import os


def get_script_path():
    frame = inspect.stack()[1]
    filename = frame[0].f_code.co_filename
    return os.path.dirname(os.path.realpath(filename))
