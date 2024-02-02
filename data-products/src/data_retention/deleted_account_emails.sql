-- Creates a list of user emails for deleted accounts
-- Adds new deleted user emails to DELETED_EMAILS
insert into USER_DATA_DELETION_DB.USER_DATA_DELETION_SCHEMA.DELETED_EMAILS
    (email)
select distinct m.email
from analytics.dbt_staging.stg_user_to_email_map as m
join USER_DATA_DELETION_DB.USER_DATA_DELETION_SCHEMA.DELETED_USERS as u on u.user_id = m.user_id
left join USER_DATA_DELETION_DB.USER_DATA_DELETION_SCHEMA.DELETED_EMAILS as e on e.email = m.email
where e.email is null;
