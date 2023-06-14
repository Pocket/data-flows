select coalesce(max(submission_timestamp), sysdate() - interval '1 hour') as current_offset
from {table_name}