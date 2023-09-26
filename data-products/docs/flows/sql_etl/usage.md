# Usage

`sql_etl` is a Prefect flow meant to act as a service for executing simple SQL based extract and load workflows.  This means a SQL statement extracts the data to cloud storage.  Then a SQL statement loads this data into a cloud-based Data Warehouse.  That is the expectation for how this should be used.

This is technically more of an `el`, but `etl` is a more common term to use.

A `sql_etl` job can be incremental and use offset logic or simply just run an extract-load process without worring about tracking offset or batching by day.

Code lives at `src/sql_etl` and is described [here](/flows/sql_etl/code).

Here is a flow diagram showing how it all works:

![](/flows/sql_etl/sql-etl-flow-diagram.jpg)

## Expectations

To leverage this service for jobs, `sql_etl` you will need to know:

- How to create a deployment
- How to create the SQL expected for things to work
- How the offset and interval logic works
- How to setup your local environment

Below we go through each element.

## Deployment

The


## SQL statments

SQL statments will live in subfolders at `src/sql_etl/sql`

Here is an example of what a set of sql job files looks like:

```
sql_etl
└── sql
    ├── backend_events_for_mozilla
    │   └── data.sql
    ├── curated_feed_exports_aurora
    │   ├── curated_feed_items
    │   │   ├── data.sql
    │   │   └── load.sql
    │   ├── curated_feed_items_deleted
    │   │   ├── data.sql
    │   │   └── load.sql
    │   ├── curated_feed_prospects
    │   │   ├── data.sql
    │   │   └── load.sql
    │   ├── curated_feed_queued_items
    │   │   ├── data.sql
    │   │   └── load.sql
    │   └── tile_source
    │       ├── data.sql
    │       └── load.sql
    ├── firefox_new_tab_impressions
    │   ├── firefox_new_tab_daily_disable_rate_by_feed
    │   │   ├── data.sql
    │   │   └── offset.sql
    │   ├── firefox_new_tab_daily_spoc_fill_rate_by_position_feed
    │   │   ├── data.sql
    │   │   └── offset.sql
    │   ├── firefox_new_tab_daily_unique_engagement_by_feed
    │   │   ├── data.sql
    │   │   └── offset.sql
    │   └── firefox_new_tab_monthly_unique_engagement_by_feed
    │       ├── data.sql
    │       └── offset.sql
    └── impression_stats_v1
        ├── data.sql
        ├── load.sql
        └── offset.sql
```

Notice that we can have up to three files possible in a folder:

- `data.sql`
- `load.sql`
- `offset.sql`

Also, notice that we support files living at the top level of a folder within the `sql` folder.  We also support a single level of nesting files in subdirectories of a folder in `sql`.  The `curated_feed_exports_aurora` is an example of the supporting nesting of subdirectories.  If, needed SQL files can live at the top of the parent directory and will executed after the subdirectories are completed.



### Extraction

At the very least your folder of SQL statements must contain a `data.sql`

This is the query which defines the data to be extracted.  

For example:
```sql
SELECT *
FROM TABLE_TO_BE_EXTRACTED;
```

By itself this will not do much and will fail. This is because we have not defined the `sql_engine` this needs to run on.  The engines this service supports are `["snowflake", "bigquery", "mysql", "postgres"]`.  All SQL is run through jinja2, which means we can add templating logic to our statements.  For `sql_engine` we need a block telling us where to run this.

For example:
```sql
{% set sql_engine = "bigquery" %}
SELECT *
FROM TABLE_TO_BE_EXTRACTED;
```

Now when the service renders and uses this extract statment, it knows where to run it.  All configuration needed for connecting the engines we support is supplied via environment variables, which will be discussed in another section.

!!! note 
    All SQL running through this service must have a `sql_engine` block.

!!! note 
    For `snowflake` and `bigquery`, we will wrap the rendered `data.sql` in specific export logic located in this method:

[`get_extraction_sql`](/flows/sql_etl/code/#src.sql_etl.run_jobs_flow.SqlEtlJob.get_extraction_sql)

 `mysql` and `postgres` will not be wrapped in any logic as of now, which means the export logic must be built into the query.

 A single `data.sql` file means you are just loading to cloud storage.

 Since these files are being formatted by jijnja2, we can add our own custom keywords to do things like this:

```sql
    SELECT
    *   
FROM
{% if for_backfill %}
    deduped_table
{% else %}
    live_table 
{% endif %}
WHERE submission_timestamp >= {{ helpers.parse_iso8601(batch_start) }}
AND submission_timestamp < {{ helpers.parse_iso8601(batch_end) }}
QUALIFY row_number() over (PARTITION BY DATE(submission_timestamp),
document_id
ORDER BY
submission_timestamp desc) = 1
```

Also, since these are jinja2 templates we can import helpers using something like this toward the top of the file:

`{% import 'helpers.j2' as helpers with context %}`

This is what makes the `helpers.parse_iso8601` call possible in the last SQL example above.

These parameters are available to all `data.sql` files:

- The fields of the base `SQLJob` pydantic model described [here](/shared/utils/code/#src.shared.utils.SqlJob/).  Your custom parameter values are defined as a dictionary in `kwargs`.
- The fields of the `SqlEtlJob` pydantic model described [here](/sql_etl/code/#src.sql_etl.run_jobs_flow.SqlEtlJob)

### Loading

Loading requires the existence of a `load.sql`.

This is the query which defines the data to be extracted.  

For example:
```sql
{% set sql_engine = "snowflake" %}
COPY INTO TABLE_TO_BE_LOADED
FROM @my_ext_stage
FILE_FORMAT = (TYPE = 'PARQUET')
```

!!! note
    Currently all data exports from `snowflake` and `bigquery` will be in `PARQUET` format.


### Offset

This service provides the option to run either `incremental` or `non-incremental` extract-load jobs.

When you have the need to produce a `last_offset` to make the flow run incremental, you will need to provide an `offset.sql` file.  This activates the incremental logic.

For example:
```sql
{% set sql_engine = "snowflake" %}
select max(timestamp_field) as last_offset
from TABLE_TO_BE_LOADED
```

Pretty much anything can be in the `offset.sql`.  For example, if you want to pin the offset to the last 24 hours you could do something like this:

```sql
{% set sql_engine = "snowflake" %}
select (trunc(sysdate()::timestamp, 'day') - interval '1 day') - interval '1 microsecond';
```

If this is run on `2023-09-17 13:00`, this would result in a value of `2023-09-15 23:59:59.999999`.  When the incremental logic built into the service add 1 microsecond, the `data.sql` file will have access to a batch_start timestamp of `2023-09-16 00:00:00.000000` for a proper `>=` where clause declaration.

There is also the option to track offset using an external table called `sql_offset_state` with the `with_external_state` flag.  If this flag is set, then `offset.sql` is not required to activate incremental logic because that value is pulled from the state table.  

!!! note
    Using the `with_external_state` does require the use of the `initial_last_offset` job parameter.

`with_external_state`, incremental logic, and job parameters will be discussed later on.  

### Intervals

The interval logic in this 

