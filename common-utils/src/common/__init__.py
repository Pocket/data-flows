import inspect
import os
from typing import Literal, Optional

import pendulum as pdm
from pendulum.parser import parse
from pydantic import BaseModel, Field, validator


def get_script_path():
    frame = inspect.stack()[1]
    filename = frame[0].f_code.co_filename
    return os.path.dirname(os.path.realpath(filename))


INTERVAL_TYPE_LITERAL = Literal["hours", "days", "weeks", "months", "years"]

class IntervalSet(BaseModel):
    start: str
    end: str


class IntervalFactory(BaseModel):
    last_offset: str = Field(
        description="last offset date or datetime for interval range.",
    )
    interval_end: Optional[str] = (
        Field(
            description=("End date or datetime for interval range."),
        ),
    )
    interval_type: Optional[INTERVAL_TYPE_LITERAL] = Field(
        description="Proper interval type to pass to factory"
    )

    class Config:
        validate_assignment = True

    @validator("interval_end")
    def set_interval_end(cls, v):
        return v or pdm.now(tz="utc").to_datetime_string()
    
    def get_intervals(self, include_now: bool = False):
        if it := self.interval_type:
            it_unit = it[:-1]
            last_offset = parse(self.last_offset)
            start = last_offset.end_of(it_unit).add(microseconds=1)
            end = parse(self.interval_end)
            period_range = pdm.period(start, end)
            intervals = [last_offset.to_datetime_string()]
            intervals_between = [
                x.to_datetime_string() for x in period_range.range(it)
            ]
            intervals.extend(intervals_between)
            if include_now:
                if end > parse(intervals[-1]):
                    intervals.append(end.to_datetime_string())
            interval_sets = []
            for idx, i in enumerate(intervals):
                end_idx = idx+1
                if end_idx == len(intervals):
                    break
                interval_sets.append(
                    IntervalSet(
                        start=i,
                        end=intervals[end_idx]
                    )
                )
            return interval_sets
