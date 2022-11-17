"""The Transform Data Module
This module holds common data transform tasks (part of the ETL process) used by Prefect.
"""
from pandas import DataFrame
import pandas as pd
from prefect import task
from html.parser import HTMLParser


@task
def df_field_strip(dataframe: pd.DataFrame, field_name: str, chars_to_remove: str=None) -> DataFrame:
    """
    This task removes the leading and the trailing characters of the string field in the DataFrame.

    Args:
        dataframe : The field from the DataFrame to perform the strip function
        field_name: the name of the field in the DataFrame
        chars_to_remove (optional): a string specifying the set of characters to be removed from the field

    Returns:
        A dataframe after applying the strip transformation
    """
    if len(dataframe) > 0:
        dataframe[field_name] = dataframe[field_name].apply(lambda x: x.strip(chars_to_remove) if x else x)
    return dataframe


class MarkupRemover(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=False)
        self.reset()
        self.convert_charrefs = True
        self.fed = []

    def handle_data(self, data):
        self.fed.append(data)

    def handle_entityref(self, name):
        self.fed.append(f'&{name};')

    def handle_charref(self, name):
        self.fed.append(f'&#{name};')

    def get_data(self):
        return ''.join(self.fed)


def get_text_from_html(html):
    remover = MarkupRemover()

    remover.feed(html)
    remover.close()
    text =  remover.get_data()
    lines = [line.strip() for line in text.split('\n') if line and not line.isspace()]
    valid_doc = (' '.join(lines)).strip()
    return valid_doc
