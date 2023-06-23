from copy import deepcopy
from pathlib import Path
from typing import Optional, Union

import pendulum as pdm
from common.settings import Settings
from jinja2 import Environment, FileSystemLoader
from pendulum import from_format
from pendulum.parser import parse
from prefect import task
from prefect.runtime import flow_run
from pydantic import BaseModel, Field


class SharedUtilsSettings(Settings):
    sql_template_path: Path


class IntervalSet(BaseModel):
    """Model to leverage for interacting with batch intervals"""

    batch_start: str = Field(description="Interval start datetime.")
    batch_end: str = Field(description="Interval end datetime.")
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
    def time_format_string(self):
        return "HH-mm-ss-SSS"

    @property
    def partition_datetime(self):
        return parse(self.batch_start)

    @property
    def partition_date_folder(self):
        date_str = self.partition_datetime.to_date_string()  # type: ignore
        return f"date={date_str}"

    @property
    def partition_time_folder(self):
        time_str = self.partition_datetime.format(self.time_format_string)  # type: ignore
        return f"time={time_str}"

    @property
    def partition_folders(self):
        return f"{self.partition_date_folder}/{self.partition_time_folder}"
    
    @property
    def partition_timestamp(self):
        return self.partition_datetime.int_timestamp # type: ignore


class SqlJob(BaseModel):
    """Model for parameters to passed to an sql job request."""

    sql_folder_name: str = Field(
        description="Relative folder name containing sql.",
    )
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
        template_path = SharedUtilsSettings().sql_template_path  # type: ignore
        render_kwargs = deepcopy(self.job_kwargs)
        render_kwargs.update(extra_kwargs)
        environment = Environment(
            loader=FileSystemLoader(f"{template_path}/{self.sql_folder_name}")
        )
        j2_env = environment
        template = j2_env.get_template(sql_file)
        return template.render(**render_kwargs)

    def get_intervals(self, last_offset: Union[str, None]) -> list[IntervalSet]:
        """Method that returns the intervals to be used for sql job.
        For job without a selections of intervals, the result will be a list
        with a single interval.

        Args:
            last_offset (str): Last offset to use for getting proper intervals.

        Returns:
            list[IntervalSet]: List of intervals to use for job processing.
            Absense of an interval type will be a list with a single interval.
        """
        # create the base interval set list
        interval_sets = []
        # use override end date if provided
        batch_end = (
            self.override_batch_end
            or flow_run.get_scheduled_start_time().to_iso8601_string()
        )
        # if last_offset is None, we use the initial offset
        if last_offset is None or last_offset == "None":
            last_offset = self.initial_last_offset
        # override last offset if provided
        last_offset_str = self.override_last_offset or str(last_offset)
        last_offset_final = parse(last_offset_str)  # type: ignore
        # the calculated intervals should start
        # from the first full
        # interval after the last offset
        # offset will be incremented by 1 millisecond
        # to support using '>=' and '<' for all intervals
        start_offset = last_offset_final.add(microseconds=1000)  # type: ignore
        start = start_offset.end_of("day").add(microseconds=1)  # type: ignore
        end = parse(batch_end)
        # create a pendulum period
        period_range = pdm.period(start, end)  # type: ignore
        # add offset as the initial item in staging list
        intervals = [start_offset]
        # create staging list of datetime object from period
        intervals_between = [x for x in period_range.range("days")]
        # combine the stanging lists
        intervals.extend(intervals_between)
        # if last interval should end through now, then append it
        if self.include_now:
            # only if its greater than the last interval datetime
            if end > intervals[-1]:  # type: ignore
                intervals.append(end)
        # create base parameters for following for loop
        interval_len = len(intervals)
        last_idx = interval_len - 2
        # create the final list of intervat sets
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
            # drop redundant initial interval
            if idx == 1:
                if start_offset == i:
                    interval_sets.pop(0)
                    is_initial = True
            interval_sets.append(
                IntervalSet(
                    batch_start=i.to_iso8601_string(),  # type: ignore
                    batch_end=intervals[end_idx].to_iso8601_string(),  # type: ignore
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
    batch_folder_datetime = interval_input.partition_datetime
    clean_up_list = []
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
        parsed_datetime = from_format(
            datetime_str, f"YYYY-MM-DD {interval_input.time_format_string}"
        )  # noqa: E501
        if parsed_datetime >= batch_folder_datetime:  # type: ignore
            if (
                parsed_datetime.format(interval_input.time_format_string)
                == "00-00-00-000"
            ):  # noqa: E501
                clean_up_list.append(date_folder_str)
                break
            clean_up_list.append(datetime_folder_str)
    return list(set(clean_up_list))
