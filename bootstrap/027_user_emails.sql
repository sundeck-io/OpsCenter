
CREATE TABLE internal.sfusers IF NOT EXISTS (name string, email string);

CREATE OR REPLACE PROCEDURE internal.refresh_users() RETURNS STRING LANGUAGE SQL AS
BEGIN
    BEGIN TRANSACTION;
        truncate table internal.sfusers;
        insert into internal.sfusers select name, email from snowflake.account_usage.users;
        call internal.set_config('SNOWFLAKE_USER_MAINTENANCE', current_timestamp()::string);
    COMMIT;
END;
