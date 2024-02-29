
CREATE OR REPLACE PROCEDURE ADMIN.DESCRIBE_SETTING(name text)
    RETURNS TEXT
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
BEGIN
    let res text := '';
    call internal.get_config(:name) into :res;
    return res;
END;

CREATE OR REPLACE PROCEDURE ADMIN.UPDATE_SETTING(name TEXT, value TEXT)
    RETURNS TEXT
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
$$
begin
    let ret text := '';
    call internal_python.python_central_proc(object_construct('name', :name, 'value', :value), 'UPDATE_SETTING') into :ret;
    return :ret;
end;
$$;

CREATE OR REPLACE PROCEDURE ADMIN.ENABLE_TASK(name TEXT)
    RETURNS TEXT
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
$$
BEGIN
    let ret text := '';
    call internal_python.python_central_proc(object_construct('name', :name), 'ENABLE_TASK') into :ret;
    return :ret;
END;
$$;

CREATE OR REPLACE PROCEDURE ADMIN.DISABLE_TASK(name TEXT)
    RETURNS TEXT
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
$$
BEGIN
    let ret text := '';
    call internal_python.python_central_proc(object_construct('name', :name), 'DISABLE_TASK') into :ret;
    return :ret;
END;
$$;

CREATE OR REPLACE PROCEDURE ADMIN.ENABLE_DIAGNOSTIC_INSTRUCTIONS()
    RETURNS TEXT
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
BEGIN
    let db text := (select current_database());
    let message text := $$
If you haven't already configured an event table for your account, follow these steps:

# Enable Event Table

These commands will create an event table and set it as the default event
table for your account. Be sure to include use a database and schema that exists in your account.

-- Double check that there is no event table already set for your account before proceeding!
SHOW PARAMETERS LIKE 'EVENT_TABLE' IN ACCOUNT;

-- Create a database
CREATE DATABASE my_database;

-- Create the event table in that database
CREATE EVENT TABLE my_database.public.my_events;

-- Set this event table as the default for your account
ALTER ACCOUNT SET EVENT_TABLE = my_database.public.my_events;

You can also follow the Snowflake instructions https://docs.snowflake.com/en/developer-guide/logging-tracing/event-table-setting-up to
set up an event table if you prefer.

# Enable Diagnostic Sharing with Sundeck for OpsCenter

Sharing diagnostics with Sundeck helps us know when users are experiencing any errors in OpsCenter
so we can fix them as soon as possible. To enable this, please run the following:

ALTER APPLICATION $$ || db || $$ SET SHARE_EVENTS_WITH_PROVIDER = true;
$$;
    return message;
END;

CREATE OR REPLACE PROCEDURE ADMIN.RELOAD_QUERY_HISTORY()
    RETURNS TEXT
    LANGUAGE SQL
    COMMENT = "Reloads the materialized query history and warehouse events data from your Snowflake ACCOUNT_USAGE database."
    EXECUTE AS OWNER
AS
BEGIN
    SYSTEM$LOG_INFO('Reloading query history and warehouse events data');
    truncate table internal.task_query_history;
    truncate table internal.task_warehouse_events;
    truncate table internal_reporting_mv.cluster_and_warehouse_sessions_complete_and_daily;
    truncate table internal_reporting_mv.query_history_complete_and_daily;
    call internal.refresh_warehouse_events(true);
    call internal.refresh_queries(true);
    return '';
END;

CREATE OR REPLACE PROCEDURE ADMIN.RELOAD_PRECONFIGURED_DATA()
    RETURNS TEXT
    LANGUAGE SQL
    COMMENT = "Recreates the Query Monitors and Labels that are included with OpsCenter without overriding any customizations you have made."
    EXECUTE AS OWNER
AS
BEGIN
    SYSTEM$LOG_INFO('Reloading preconfigured data');
    call internal.merge_predefined_probes();
    call internal.merge_predefined_labels();
    return '';
END;
