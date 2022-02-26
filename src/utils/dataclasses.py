import dataclasses
import json


class Missing:
    """
    Type that can be used to signify that the field is missing (not set to any value).
    Note, Optional is not the same as Missing:
    - Optional[str] can be str or None
    - Union[str, Missing] = Missing() can be str or Missing.

    There's some discussion at Pydantic about supporting missing fields with Python 3.10, but as of 3.9 I could not
    find an existing solution for either dataclasses or Pydantic.
    """
    pass


class DataClassJSONEncoder(json.JSONEncoder):
    """
    Allows a dataclass to be encoded as json using json.dumps(foo, cls=DataClassJSONEncoder)
    Fields set to `Missing()` will not be present in the output.
    """
    def default(self, o):
        if dataclasses.is_dataclass(o):
            d = dataclasses.asdict(o)
            return DataClassJSONEncoder._without_missing(d)
        return super().default(o)

    @staticmethod
    def _without_missing(value):
        """
        Recursively removes all `Missing` values from a dict or list.
        :param value: Dict or list input.
        :return: value without missing values (if value is not a dict or list, it's returned as-is)
        """
        if type(value) is dict:
            return {
                k: DataClassJSONEncoder._without_missing(v)
                for k, v in value.items() if not isinstance(v, Missing)
            }
        elif type(value) is list:
            return [
                DataClassJSONEncoder._without_missing(v)
                for v in value if not isinstance(v, Missing)
            ]
        else:
            return value
