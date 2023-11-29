import functools
from unittest.mock import patch, AsyncMock

import pandas as pd

async_patch = functools.partial(patch, new_callable=AsyncMock)


class SameDf:
    """
    Usage:
    ```python
    mock.method.assert_called_once_with(SAME_DF(pd.DataFrame({
        'name': ['Eric', 'Yoav'],
        'age': [28, 34]
    })))
    ```

    Source: https://stackoverflow.com/a/69010217
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df

    def __eq__(self, other):
        return isinstance(other, pd.DataFrame) and other.equals(self.df)
