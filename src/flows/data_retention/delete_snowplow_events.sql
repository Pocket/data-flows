-- Removes Snowplow Events (SNOWPLOW.ATOMIC.EVENTS) for deleted accounts
delete from EVENTS as e
using development.gaurang_data_retention_deletion.DELETED_USERS as d
where ( d.hashed_user_id = e.CONTEXTS_COM_POCKET_USER_1[0]:hashed_user_id
        or  d.user_id = e.CONTEXTS_COM_POCKET_USER_1[0]:user_id )
