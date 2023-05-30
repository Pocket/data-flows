select *
from {database_name}.{schema_name}.{table_name}
where {offset_key} > '{current_offset}'
and {offset_key} <= '{new_offset}'
and {offset_key} > current_timestamp - interval '24 hours'


