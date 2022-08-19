# References
- [Service page in Confluence](https://getpocket.atlassian.net/wiki/spaces/PE/pages/2671771649/Braze+Pocket+Hits+data+pipeline)
- [Dbt docs for the STG_BRAZE_USER_DELTAS model](https://cloud.getdbt.com/accounts/4171/jobs/35797/docs/#!/model/model.pocket.stg_braze_user_deltas#columns)

# Flow use-case
- Update user data in Braze on a schedule based on the `STG_BRAZE_USER_DELTAS` Dbt model.
- Backfill Snowplow data, or data that matches the schema of the `STG_BRAZE_USER_DELTAS` Snowflake table.

# Parameters
The following parameters can be modified in the Prefect UI when running this flow manually. The default values are used
when the flow is run on a schedule.

## Snowflake parameters (string)
Override the following Snowflake parameter to backfill from a different Snowflake schema, database, and/or table.
- `snowflake_schema` (string): Snowflake schema  
- `snowflake_database` (string): Snowflake database  
- `snowflake_table_name` (string): Snowflake table name

## max_operations_per_task_run (integer)
Controls the number of rows processed per task run, which affects execution speed and whether data is processed FIFO.

Example values:
- **100000 (default)** lets tasks process chunks of up to 100,000 rows. The `STG_BRAZE_USER_DELTAS` Snowflake table grows by about 350 rows per hour on average, so scheduled runs do not leverage parallelism because it would take over a week for this many rows to accumulate. The benefit of this is that we guarantee FIFO processing of events, which is important because the order of Snowplow events matters.
- **1000** is the performance sweet-spot, without using too many Prefect tasks, but with no guarantee of FIFO if there are more than 1,000 rows to be processed. When doing a backfill where order of processing does not matter, and execution time needs to be minimized, a value of 1,000 would be a good choice.
- **50 (not recommended)** Every request to Braze would be executed in parallel in a separate Prefect task. Our Prefect contract includes 3,000,000 task runs per month, so it might be costly to start a new task for every 50 rows. There is no guarantee of FIFO if there are more than 50 rows to be processed.
- If max_operations_per_task_run is set to near-infinite (say 999999999), then FIFO is guaranteed, but it might also take hours to process many millions of rows.
