import os
from copy import deepcopy
from pathlib import Path

import pendulum as pdm
from common import get_script_path
from common.cloud.gcp_utils import MozGcpCredentials
from common.databases.snowflake_utils import (
    SfGcsStage,
    SnowflakeGcsStageSettings,
    get_gcs_stage,
)
from common.deployment.worker import FlowDeployment, FlowSpec
from common.settings import CommonSettings, get_cached_settings
from jinja2 import Environment, FileSystemLoader, Template
from pendulum.parser import parse as pdm_parse
from prefect import flow, get_run_logger, task
from shared.utils import (
    IntervalSet,
    SharedUtilsSettings,
    SqlJob,
    SqlStmt,
    get_files_for_cleanup,
)

CS = CommonSettings()  # type: ignore


class JobConfig(BaseModel):
    sql_folder: str
    include_now: bool = False
    extras: dict = {}


JOB_CONFIG = []

LEGACY_HOURLY = JobConfig(sql_folder="firefox_new_tab_impressions_daily")
LEGACY_DAILY = JobConfig(
    sql_folder="firefox_new_tab_impressions_hourly", include_now=True
)
GLEAN_HOURLY = JobConfig(sql_folder="glean_firefox_new_tab_impressions_daily")
GLEAN_DAILY = JobConfig(
    sql_folder="glean_firefox_new_tab_impressions_hourly", include_now=True
)

SQL_LOCATION = os.path.join(get_script_path(), "sql")


def render_sql_string(sql_string: str, extra_kwargs: dict = {}) -> str:
    """Helper method for rendering a jinj2 sql string
    using job kwargs plus optional additional kwargs.

    Args:
        sql_string (str): SQL text with jijna2 template logic.
        extra_kwargs (dict): Optional kwargs to pass into template on top of
        the job_kwargs.

    Returns:
        SqlStmt: SqlStmt object with sql text and db engine.
    """
    environment = Environment()
    j2_env = environment
    template = j2_env.from_string(sql_string)
    return template.render(**extra_kwargs)


def render_sql_file(sql_file: str, sql_file_path: str, extra_kwargs: dict = {}) -> str:
    """Helper method for rendering a jinja2 sql file using
    job kwargs plus optional additional kwargs.

    Args:
        sql_file (str): File name containing SQL text with jijna2 template logic.
        extra_kwargs (dict): Optional kwargs to pass into template on top of
        the job_kwargs.

    Returns:
        SqlStmt: SqlStmt object with sql text and db engine.
    """

    environment = Environment(
        loader=FileSystemLoader([sql_file_path]),
    )
    j2_env = environment
    template = j2_env.get_template(sql_file)
    return template.render(**extra_kwargs)


def create_extraction(
    sql_file: str,
    gcp_creds: MozGcpCredentials,
    interval_date: pdm.datetime.DateTime,
    extra_kwargs: dict = {},
):
    sql_path_obj = Path(SQL_LOCATION, sql_file)
    file = sql_path_obj.name
    file_stem = sql_path_obj.stem
    path = str(sql_path_obj.parent)

    bq_extraction_sql = """EXPORT DATA OPTIONS(
          uri='gs://{{ gcs_uri }}',
          format='PARQUET',"
          compression='SNAPPY',
          overwrite=true) AS
        {{ sql }}"""  # noqa: E501

    extract_kwargs = {
        "gcs_uri": os.path.join(
            gcp_creds.staging_bucket,
            file_stem,
            str(pdm.now(tz="UTC").int_timestamp),
            f"date={interval_date.to_date_string()}",
            "data*.parq",
        )
    }
    sql_query = render_sql_file(file, path, extra_kwargs=extra_kwargs)
    extract_kwargs["sql"] = sql_query
    extraction_sql_stmt = render_sql_string(bq_extraction_sql, extra_kwargs=extract_kwargs)
    return extraction_sql_stmt


def create_load():
    load_template = """create temporary table {{ table_name }}_tmp (
        {{ table_def }},
        {{ metadata_column_definitions }}
    );

    {% set columns = table_def.split(',') %}

    copy into {{ table_name }}_tmp 
    from (select
            {% for c in columns %}
                {% set column_def = c.split() %}
                $1:{{ column_def[0] }}::{{ column_def[1] }},
            {% endfor %}
            {{ metadata_values }}
        from {{ snowflake_stage_uri }}
    );

    create table if not exists {{ table_name }}  like {{ table_name }}_tmp;

    set merge_key = (select min({{merge_key}}) from {{ table_name }}_tmp);
    
    begin;
    delete from {{ table_name }} 
    where {{ merge_key }} = $merge_key;
    insert into {{ table_name }} 
    select * from {{ table_name }}_tmp;
    commit;"""


def get_intervals(
    batch_start: str, batch_end: str | None = None, include_now: bool = False
):
    if batch_end:
        end_datetime = pdm_parse(batch_end)
    else:
        end_datetime = pdm.now(tz="UTC")
    start_datetime = pdm_parse(batch_start)
    period_range = pdm.period(
        start_datetime.start_of("day"), end_datetime.start_of("day"), absolute=True
    )
    intervals = [x.to_datetime_string() for x in period_range.range("days")]
    if not include_now:
        intervals.pop()
    return intervals


print(get_intervals("2024-06-26", include_now=True))
