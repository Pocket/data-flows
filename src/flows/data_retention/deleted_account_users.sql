-- Creates a list of hashed_user_id and user_id for deleted accounts
-- Adds new deleted user accounts to DELETED_USERS
insert into DELETED_USERS
    (hashed_user_id, user_id)
select distinct a.hashed_user_id, a.user_id
from analytics.dbt_staging.stg_account_deletions as a
left join DELETED_USERS as b
    on b.hashed_user_id = a.hashed_user_id and b.user_id = a.user_id
where b.hashed_user_id is null;
