from prefect import Flow
from prefect.tasks.gcp.bigquery import BigQueryTask

# This Flow should do the following:
#   - Export Firefox engagement data from BigQuery to GCS
#   - Determine and Setup a schedule/interval for the export (need to determine a criteria):
#           - Entire day partition on "submission_timestamp"?
#           - How do we ensure we are not exporting duplicates?

