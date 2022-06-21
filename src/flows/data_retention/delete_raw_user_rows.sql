
-- BEGIN: RAW.FIREHOSE

DELETE FROM AB_TEST_TRACK as s
USING development.gaurang_data_retention_deletion.DELETED_USERS as d
WHERE s.EVENT:data:user_key = d.user_id
AND s.EVENT:data:user_key_type_id = 1;

DELETE FROM ITEM_SESSION as s
USING development.gaurang_data_retention_deletion.DELETED_USERS as d
WHERE s.EVENT:data:user_id = d.user_id;

DELETE FROM PINPOINT as s
USING development.gaurang_data_retention_deletion.DELETED_USERS as d
WHERE s.EVENT:endpoint:User:UserId::int = d.user_id;

DELETE FROM POCKET_V3_SEND as s
USING development.gaurang_data_retention_deletion.DELETED_USERS as d
WHERE s.EVENT:data:user_id = d.user_id;

DELETE FROM PREMIUM_SUBSCRIPTION_CREATED as s
USING development.gaurang_data_retention_deletion.DELETED_USERS as d
WHERE s.EVENT:data:user_id = d.user_id;

DELETE FROM PREMIUM_SUBSCRIPTION_ENDED as s
USING development.gaurang_data_retention_deletion.DELETED_USERS as d
WHERE s.EVENT:data:user_id = d.user_id;

DELETE FROM PREMIUM_SUBSCRIPTION_RENEWED as s
USING development.gaurang_data_retention_deletion.DELETED_USERS as d
WHERE s.EVENT:data:user_id = d.user_id;

DELETE FROM SENDGRID_EVENTS as s
USING development.gaurang_data_retention_deletion.DELETED_EMAILS as d
WHERE s.EVENT:email = d.email;

-- TODO: UNIFIED_EVENT_ALL (If this is not used, can we drop all data and stop the Snowpipe from loading new data?)

DELETE FROM USER_ACTION as s
USING development.gaurang_data_retention_deletion.DELETED_USERS as d
WHERE s.EVENT:data:user_id = d.user_id;

DELETE FROM USER_ITEM_TAGS_ADDED as s
USING development.gaurang_data_retention_deletion.DELETED_USERS as d
WHERE s.EVENT:data:user_id = d.user_id;

DELETE FROM USER_LIST_ITEM_CREATED as s
USING development.gaurang_data_retention_deletion.DELETED_USERS as d
WHERE s.EVENT:data:user_id = d.user_id;

DELETE FROM USER_TRACK_ADV as s
USING development.gaurang_data_retention_deletion.DELETED_USERS as d
WHERE s.EVENT:data:user_id = d.user_id;

DELETE FROM WEB_TRACK as s
USING development.gaurang_data_retention_deletion.DELETED_USERS as d
WHERE s.EVENT:data:user_id = d.user_id;
