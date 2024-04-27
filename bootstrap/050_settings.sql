
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
BEGIN
    let pass boolean := (select :name is not null and :value is not null and :name in ('default_timezone', 'storage_cost', 'serverless_credit_cost', 'compute_credit_cost'));
    if (not pass) then
        return 'Invalid setting name or value, setting name cannot be null and value cannot be null. Setting name cannot be one of: default_timezone, storage_cost, serverless_credit_cost, compute_credit_cost';
    end if;
    let is_tz boolean := (select :name = 'default_timezone');
    if (is_tz) then
        begin
            SELECT CONVERT_TIMEZONE('America/Los_Angeles', :value::varchar, '2019-01-01 14:00:00'::timestamp_ntz) AS conv;
        exception
            when other then
                return 'Invalid setting value, setting value valid timezone string.';
        end;
    else
        let correct_type boolean := (select try_cast(:value as number) is not null);
        if (not correct_type) then
            return 'Invalid setting value, setting value must be a number.';
        end if;
    end if;

	call internal.set_config(:name, :value);
	return '';
END;

CREATE OR REPLACE PROCEDURE ADMIN.ENABLE_TASK(name TEXT)
    RETURNS TEXT
    LANGUAGE PYTHON
    RUNTIME_VERSION = "3.10"
    HANDLER = 'run'
    PACKAGES = ('snowflake-snowpark-python', 'pydantic==1.*')
    IMPORTS = ('{{stage}}/python/crud.zip')
    EXECUTE AS OWNER
AS
$$
from crud.errors import summarize_error
from crud.tasks import Task
def run(session, name: str):
    try:
        task = Task(task_name=name)
        task.enable(session)
        return ""
    except Exception as e:
        return summarize_error("Unable to enable task", e)
$$;

CREATE OR REPLACE PROCEDURE ADMIN.DISABLE_TASK(name TEXT)
    RETURNS TEXT
    LANGUAGE PYTHON
    RUNTIME_VERSION = "3.10"
    HANDLER = 'run'
    PACKAGES = ('snowflake-snowpark-python', 'pydantic==1.*')
    IMPORTS = ('{{stage}}/python/crud.zip')
    EXECUTE AS OWNER
AS
$$
from crud.errors import summarize_error
from crud.tasks import Task
def run(session, name: str):
    try:
        task = Task(task_name=name)
        task.disable(session)
        return ""
    except Exception as e:
        return summarize_error("Unable to disable task", e)
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
    truncate table internal.task_log;
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
