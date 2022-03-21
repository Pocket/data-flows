import dataclasses
import json
from typing import Any


class DataClassJSONEncoderWithoutNoneValues(json.JSONEncoder):
    """
    Allows a dataclass to be encoded as json using json.dumps(foo, cls=DataClassJSONEncoder)
    Fields set to `Missing()` will not be present in the output.
    """
    def default(self, o):
        if dataclasses.is_dataclass(o):
            d = dataclasses.asdict(o)
            return _without_none_values(d)
        return super().default(o)


def _without_none_values(obj: Any):
    """
    Returns copy of input with all None values removed, even recursively in sequences.

    Code is based on a Stackoverflow answer: https://stackoverflow.com/a/20558778/331030, but unlike this answer,
    keys that are None are not removed from dictionaries, only None values are.

    :param obj: list, tuple, set, dict, or primitive types (str, int, float, etc.)
    :return: A copy of obj without any None values, including recursively into list, tuple, set, dict.
    """
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(_without_none_values(x) for x in obj if x is not None)
    elif isinstance(obj, dict):
        return {k: _without_none_values(v) for k, v in obj.items() if v is not None}
    else:
        return obj
