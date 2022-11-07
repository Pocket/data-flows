"""The Transform Data Module
This module holds common data transform tasks (part of the ETL process) used by Prefect.
"""
from pandas import DataFrame
import pandas as pd
from prefect import task
import html2text as html2text
import re

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

class HtmlToText():
    def __init__(self):
        h = html2text.HTML2Text()
        h.ignore_links = True
        h.ignore_images = True
        h.ignore_emphasis = True
        h.body_width = 1024
        h.strong_mark = ''
        h.emphasis_mark = ''
        h.ul_item_mark = ''

        re_dummy = re.compile('(.^)(.^)')
        html2text.config.RE_MD_BACKSLASH_MATCHER = re_dummy
        html2text.config.RE_MD_DOT_MATCHER = re_dummy
        html2text.config.RE_MD_PLUS_MATCHER = re_dummy
        html2text.config.RE_MD_DASH_MATCHER = re_dummy

        self.h = h

    def get_text(self, html):
        text = self.h.handle(str(html))

        # Remove heading markdown
        text = re.sub(r"#+\s+", "", text)
        # Remove table markdown (ignore_tables doesn't add spacing inbetween columns)
        text = re.sub(r"-+\|[\-\|]+ *\n", "", text)
        text = re.sub(r"\| ", "\n", text)
        # Remove horizontal rules
        text = re.sub(r"\* \* \*\n\n", "", text)
        # Remove trailing whitespace
        text = re.sub(r"\s*?(\n{1,2})\s*", r"\1", text)
        text = text.strip()

        return text


def get_text_from_html(html):
    return HtmlToText().get_text(html)
