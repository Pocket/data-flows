--
-- BEGIN: FIVETRAN.STRIPE
use schema FIVETRAN.STRIPE;

DELETE FROM CUSTOMER as s
where IS_DELETED = true;

DELETE FROM CARD as s
where IS_DELETED = true;
