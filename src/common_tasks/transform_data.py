from pandas import DataFrame
import pandas as pd
from prefect import task

@task
def df_field_strip(dataframe: pd.DataFrame, field_name: str, chars_to_remove: str=None) -> DataFrame:
    if len(dataframe) > 0:
        dataframe[field_name] = dataframe[field_name].apply(lambda x: x.strip(chars_to_remove) if x else x)
    return dataframe
