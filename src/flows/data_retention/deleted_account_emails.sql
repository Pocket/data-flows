-- Creates a list of user emails for deleted accounts
-- Adds new deleted user emails to DELETED_EMAILS
insert into DELETED_EMAILS
    (email)
select distinct m.email
from analytics.dbt_staging.stg_user_to_email_map as m
join DELETED_USERS as u on u.user_id = m.user_id
left join DELETED_EMAILS as e on e.email = m.email
where e.email is null;
