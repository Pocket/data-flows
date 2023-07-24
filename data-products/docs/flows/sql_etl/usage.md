# Usage

`sql_etl` is a Prefect flow meant to act as a service for executing simple SQL based extract and load workflows.  This means a SQL statement extracts the data to cloud storage.  Then a SQL statement loads this data into a cloud-based Data Warehouse.  That is the expectation for this should be used.

This is technically more of an `el`, but `etl` is a more common term to use.

Code lives at `src/sql_etl` and is described [here](/flows/sql_etl/code).

A `sql_etl` job consists of 2 elements:

- a folder containing SQL statements
- a Prefect deployment configuration

Below we go through each element.

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

- data.sql
- load.sql
- offset.sql

Also, notice that we support files liveing at the top level of a folder within the `sql` folder.  We also support a single level of nesting files in subdirectories of a folder in `sql`.

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

NOTE: All SQL running through this service must have a `sql_engine` block.

NOTE: For `snowflake` and `bigquery`, we will wrap the rendered `data.sql` in specific export logic located in this method:

[`get_extraction_sql`](/flows/sql_etl/code/#src.sql_etl.run_jobs_flow.SqlEtlJob.get_extraction_sql)

 `mysql` and `postgres` will not be wrapped in any logic as of now, which means the export logic must be built into the query.

 A single `data.sql` file means you are just loading to cloud storage.

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

NOTE:  Currently all data exports from `snowflake` and `bigquery` will be in `PARQUET` format.


### Offset

This service provides the option to run either `incremental` or `non-incremental` extract-load jobs, via the `is_incremental` flag.

When you have the ability to pull the `last_offset` from the destination table on an incremental job, you will need to provide an `offset.sql` file.

For example:
```sql
{% set sql_engine = "snowflake" %}
select max(timestamp_field) as last_offset
from TABLE_TO_BE_LOADED
```

There is also the option to track offset using an external table called `sql_offset_state` with the `with_external_state` flag.

`is_incremental` and `with_external_state` flags will discussed in the Prefect deployment configuration section.
