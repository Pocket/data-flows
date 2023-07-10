import os
from copy import deepcopy
from pathlib import Path
from typing import Optional, Union

import pendulum as pdm
from common.settings import Settings
from jinja2 import Environment, FileSystemLoader
from pendulum import from_format
from pendulum.datetime import DateTime
from pendulum.parser import parse
from prefect import task
from pydantic import BaseModel, Field, PrivateAttr


class SharedUtilsSettings(Settings):
    """Setting model to define reusable settings."""

    sql_template_path: Optional[str]


class IntervalSet(BaseModel):
    """Model to leverage for interacting with batch intervals"""

    batch_start: str = Field(description="Interval start datetime.")
    batch_end: str = Field(description="Interval end datetime.")
    first_interval_start: str = Field(description="First full day in interval list.")
    sets_end: Optional[str] = Field(
        description="Batch end used to stop at.",
    )
    is_initial: bool = Field(
        description=(
            "Flag to use for SQL template because the initial "
            "start filter will '>' as opposed to '>='."
        )
    )
    is_final: bool = Field(
        description=(
            "Flag to use for SQL template because the final interval "
            "may trigger special logic for offset."
        )
    )

    @property
    def time_format_string(self) -> str:
        """Helper for getting the time format string used.

        Returns:
            str: time format string used for folder.
        """
        return "HH-mm-ss-SSS"

    @property
    def partition_datetime(self) -> DateTime:
        """Helper for getting the DateTime object used for defining partition

        Returns:
            DateTime: DateTime object used for defining partition.
        """
        return parse(self.batch_start)  # type: ignore

    @property
    def partition_date_folder(self) -> str:
        """Helper for getting the partition date folder used.

        Returns:
            str: Partition date folder used.
        """
        date_str = self.partition_datetime.to_date_string()  # type: ignore
        return f"date={date_str}"

    @property
    def partition_time_folder(self) -> str:
        """Helper for getting the partition time folder used.

        Returns:
            str: Partition time folder used.
        """
        time_str = self.partition_datetime.format(self.time_format_string)  # type: ignore
        return f"time={time_str}"

    @property
    def partition_folders(self) -> str:
        """Helper for getting date time partition folders.

        Returns:
            str: Date time partition folders.
        """
        return f"{self.partition_date_folder}/{self.partition_time_folder}"

    @property
    def partition_timestamp(self) -> int:
        """Helper for getting unix timestamp as an int.

        Returns:
            int: Partition unix timestamp int.
        """
        return self.partition_datetime.int_timestamp  # type: ignore


class SqlJob(BaseModel):
    """Model for parameters to passed to an sql job request."""

    sql_folder_name: str = Field(
        description="Relative folder name containing sql.",
    )
    # setting sql_templat_path as private attribute to allow for...
    # easier override in child classes via _init_private_attributes
    _sql_template_path: Optional[str] = PrivateAttr()
    initial_last_offset: Optional[str] = Field(
        description="Optional initial batch start offset.",
    )
    override_last_offset: Optional[str] = Field(
        description="Optional batch start override to backfill as needed.",
    )
    override_batch_end: Optional[str] = Field(
        description="Optional batch end override to hard stop at.",
    )
    include_now: bool = Field(
        False,
        description=(
            "Whether to include current_time (for manual runs) "
            "or scheduled start datetime."
        ),
    )
    kwargs: dict = Field(
        {},
        description=(
            "Any additional keyword arguments as a dictionary"
            "to pass into your templates."
        ),
    )

    def _init_private_attributes(self) -> None:
        """Great way to update private attributes without
        losing autocomplete on the model.

        https://github.com/pydantic/pydantic/discussions/3512#discussioncomment-3226167

        """
        super()._init_private_attributes()
        self._sql_template_path = SharedUtilsSettings().sql_template_path  # type: ignore  # noqa: E501

    @property
    def job_kwargs(self) -> dict:
        """Returns a flat dictionary of the kwargs parameter
        plus the other top level parameters.  This gets passed to
        the tempate rendering methods.

        Returns:
            dict: Model parameters as a single dict with kwargs items inline.
        """
        top_level_kwargs = deepcopy(self.dict())
        top_level_kwargs.pop("kwargs")
        job_kwargs = deepcopy(self.kwargs)
        job_kwargs.update(top_level_kwargs)
        return job_kwargs

    def render_sql_string(self, sql_string: str, extra_kwargs: dict = {}) -> str:
        """Helper method for rendering a jinj2 sql string
        using job kwargs plus optional additional kwargs.

        Args:
            sql_string (str): SQL text with jijna2 template logic.
            extra_kwargs (dict): Optional kwargs to pass into template on top of
            the job_kwargs.

        Returns:
            str: Rendered sql text.
        """
        render_kwargs = deepcopy(self.job_kwargs)
        render_kwargs.update(extra_kwargs)
        environment = Environment()
        j2_env = environment
        template = j2_env.from_string(sql_string)
        return template.render(**render_kwargs)

    def render_sql_file(self, sql_file: str, extra_kwargs: dict = {}) -> str:
        """Helper method for rendering a jinja2 sql file using
        job kwargs plus optional additional kwargs.

        Args:
            sql_file (str): File name containing SQL text with jijna2 template logic.
            extra_kwargs (dict): Optional kwargs to pass into template on top of
            the job_kwargs.

        Returns:
            str: Rendered sql text.
        """
        template_path = self._sql_template_path
        render_kwargs = deepcopy(self.job_kwargs)
        render_kwargs.update(extra_kwargs)
        environment = Environment(
            loader=FileSystemLoader(f"{template_path}/{self.sql_folder_name}")
        )
        j2_env = environment
        template = j2_env.get_template(sql_file)
        return template.render(**render_kwargs)

    def get_intervals(self, last_offset: Union[str, None] = None) -> list[IntervalSet]:
        """Method that returns the intervals to be used for sql job.
        For job without a selections of intervals, the result will be a list
        with a single interval.

        Expectations, unless include_now is True, is the last interval in the list
        will be the last full interval in the range.  For example, since we only
        supporting days, if the last offset is today, there should be no intervals
        to process, becuase the default is up through yesterday (UTC). If include_now
        is True, then that final interval in the list will end before now (UTC) or the
        end_date_override.

        Args:
            last_offset (str): Last offset to use for getting proper intervals.

        Returns:
            list[IntervalSet]: List of intervals to use for job processing.
            Absense of an interval type will be a list with a single interval.
        """
        # use override end date if provided
        batch_end = self.override_batch_end or pdm.now(tz="utc").to_iso8601_string()
        # if last_offset is None, we use the initial offset
        if last_offset is None or last_offset == "None":
            last_offset = self.initial_last_offset
        # override last offset if provided
        last_offset_str = self.override_last_offset or str(last_offset)
        # offset will be incremented by 1 microseconds
        # to support using '>=' and '<' for all intervals
        last_offset_final = parse(last_offset_str).add(microseconds=1)  # type: ignore
        # the calculated intervals should start
        # from the first full
        # interval after the last offset
        start = last_offset_final.end_of("day").add(microseconds=1)  # type: ignore
        end = parse(batch_end)
        # get the previous
        # create a pendulum period
        period_range = pdm.period(start, end, absolute=True)  # type: ignore
        # create base list of datetime object from period
        intervals = [
            x
            for x in period_range.range("days")
            if x <= end.start_of("day")  # type: ignore
        ]
        if intervals:
            # if we have intervals, insert offset
            intervals.insert(0, last_offset_final)
        # setting base end to the last item in list
        base_end = intervals[-1]
        # only append base_end if needed and > then base_end
        if self.include_now or self.override_batch_end:
            if end > base_end:  # type: ignore
                intervals.append(end)
                base_end = end
        # create base parameters for the for loop
        interval_len = len(intervals)
        last_idx = interval_len - 2
        # create the final list of intervat sets
        interval_sets = []
        for idx, i in enumerate(intervals):
            is_initial = False
            is_final = False
            if idx == 0:
                is_initial = True
            if idx == last_idx:
                is_final = True
            end_idx = idx + 1
            # last item in list should not be used as start
            if end_idx == interval_len:
                break
            interval_sets.append(
                IntervalSet(
                    batch_start=i.to_iso8601_string(),  # type: ignore
                    batch_end=intervals[end_idx].to_iso8601_string(),  # type: ignore
                    first_interval_start=start.to_iso8601_string(),  # type: ignore
                    sets_end=base_end.to_iso8601_string(),  # type: ignore
                    is_initial=is_initial,
                    is_final=is_final,
                )
            )

        return interval_sets


@task()
def get_files_for_cleanup(
    file_list: list[tuple],
    interval_input: IntervalSet,
    block_storage_prefix: str = "gcs",
) -> list[str]:
    """_summary_

    Args:
        file_list (list[tuple]): File list pulled from warehouse stage.
        interval_input (IntervalSet): Interval metadata.
        block_storage_prefix (str, optional): Block storage prefex used for files.
        This helps for deconstructing the path. Defaults to "gcs".

    Returns:
        list[str]: List of staged file paths for deletion.
    """
    # get the timestamp used for the partition folders
    batch_folder_datetime = interval_input.partition_datetime
    # get datetime object for first full day interval start
    # we will delete all file paths after the base start
    date_folder_base_start = parse(interval_input.first_interval_start)
    date_folder_base_end = parse(interval_input.sets_end)  # type: ignore
    # initial list for collecting file paths
    clean_up_list = []
    # loop through the input file and append file paths to final results...
    # as needed.
    for f in file_list:
        file_str = f[0].replace(f"{block_storage_prefix}://", "")
        file_parts = Path(file_str).parts
        date_folder_str = file_parts[2]
        datetime_folder_str = file_parts[2] + "/" + file_parts[3]
        datetime_str = (
            datetime_folder_str.replace("date=", "")
            .replace("time=", "")
            .replace("/", " ")
        )
        # the goal of above is to infer datetime from the existing file paths
        parsed_datetime = from_format(
            datetime_str, f"YYYY-MM-DD {interval_input.time_format_string}"
        )  # noqa: E501
        # only delete files after interval start and before last interval end
        if parsed_datetime >= batch_folder_datetime:  # type: ignore
            if parsed_datetime < date_folder_base_end:  # type: ignore
                # if the datetime from folder is greater than the first full day start...  # noqa: E501
                # add the date folder and break loop as there is no need to check any more  # noqa: E501
                if parsed_datetime >= date_folder_base_start:  # type: ignore
                    clean_up_list.append(date_folder_str)
                    break
                clean_up_list.append(datetime_folder_str)
    return list(set(clean_up_list))


if __name__ == "__main__":
    t = SqlJob(
        sql_folder_name="test",
        override_last_offset="2023-06-17 21:59:59.999",
        override_batch_end="2023-06-26 02:00:00",
        include_now=True,
    )
    print(t.get_intervals())
