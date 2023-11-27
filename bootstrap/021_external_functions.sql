
-- This gets overwritten with a real function if the user sets it up.
-- We have to wrap and catch here because snowflake ignores the if not exists if the function object is an external function.
BEGIN
    CREATE FUNCTION IF NOT EXISTS internal.ef_qlike(request object)
        RETURNS VARIANT
        LANGUAGE JAVASCRIPT
    AS 'throw "You must configure a Sundeck token to use QLike.";';
EXCEPTION
    WHEN statement_error THEN
        let isalreadyef boolean := (select CONTAINS(:SQLERRM, 'API_INTEGRATION') AND CONTAINS(:SQLERRM, 'must be specified'));
        if (not isalreadyef) then
            RAISE;
        end if;
    WHEN OTHER THEN
        RAISE;
END;

CREATE or replace FUNCTION internal.throw_exception(ERR varchar)
    RETURNS VARIANT
    LANGUAGE JAVASCRIPT
AS $$
throw ERR;
$$;

create or replace function internal.wrapper_qlike(request object)
    returns boolean
    immutable
as
$$
    iff(length(internal.ef_qlike(request):error) != 0,
        internal.throw_exception(internal.ef_qlike(request):error),
        internal.ef_qlike(request):result)::boolean
$$;

create or replace function tools.qlike(query_text varchar, selector varchar)
    returns boolean
as
$$
    internal.wrapper_qlike(object_construct('selector', selector, 'query_text', query_text, 'database', current_database(), 'schema', current_schema()))
$$;

create or replace function tools.qlike(query_text varchar, selector varchar, params varchar)
    returns boolean
as
$$
    internal.wrapper_qlike(object_construct('selector', selector, 'query_text', query_text, 'database', current_database(), 'schema', current_schema(), 'selector_params', params))
$$;

create or replace function tools.qlike(query_text varchar, selector varchar, database varchar, currentschema varchar)
    returns boolean
as
$$
    internal.wrapper_qlike(object_construct('selector', selector, 'query_text', query_text, 'database', database, 'schema', currentschema))
$$;

create or replace function tools.qlike(query_text varchar, selector varchar, params varchar, database varchar, currentschema varchar)
    returns boolean
as
$$
    internal.wrapper_qlike(object_construct('selector', selector, 'query_text', query_text, 'database', database, 'schema', currentschema, 'selector_params', params))
$$;

BEGIN
    CREATE FUNCTION IF NOT EXISTS internal.ef_notifications(request object)
        RETURNS VARIANT
        LANGUAGE JAVASCRIPT
    AS 'throw "You must configure a Sundeck token to use Notifications.";';
EXCEPTION
    WHEN statement_error THEN
        let isalreadyef boolean := (select CONTAINS(:SQLERRM, 'API_INTEGRATION') AND CONTAINS(:SQLERRM, 'must be specified'));
        if (not isalreadyef) then
            RAISE;
        end if;
    WHEN OTHER THEN
        RAISE;
END;


create or replace function internal.wrapper_notifications(request object)
returns variant
immutable
as
$$
iff(length(internal.ef_notifications(request):error) != 0,
    internal.throw_exception(internal.ef_notifications(request):error),
    internal.ef_notifications(request):failures)
$$;

create or replace function internal.notifications(body varchar, subject varchar, type varchar, receiver varchar)
    returns variant
as
$$
        internal.wrapper_notifications(object_construct('body', body, 'subject', subject, 'to', receiver, 'message_type', type))
$$;

BEGIN
    CREATE FUNCTION IF NOT EXISTS internal.ef_run(unused object, request object)
        RETURNS VARIANT
        LANGUAGE JAVASCRIPT
    AS 'throw "You must configure a Sundeck token to use this.";';
EXCEPTION
    WHEN statement_error THEN
        let isalreadyef boolean := (select CONTAINS(:SQLERRM, 'API_INTEGRATION') AND CONTAINS(:SQLERRM, 'must be specified'));
        if (not isalreadyef) then
            RAISE;
        end if;
    WHEN OTHER THEN
        RAISE;
END;

-- Quota reporting to Sundeck EF
BEGIN
    CREATE FUNCTION IF NOT EXISTS internal.ef_report_quota_used(request object)
        RETURNS VARIANT
        LANGUAGE JAVASCRIPT
    AS 'throw "You must configure a Sundeck token before reporting quota consumption.";';
EXCEPTION
    WHEN statement_error THEN
        let isalreadyef boolean := (select CONTAINS(:SQLERRM, 'API_INTEGRATION') AND CONTAINS(:SQLERRM, 'must be specified'));
        if (not isalreadyef) then
            RAISE;
        end if;
    WHEN OTHER THEN
        RAISE;
END;

-- Add service account to Sundeck
BEGIN
    CREATE FUNCTION IF NOT EXISTS internal.ef_register_service_account(request object)
        RETURNS VARIANT
        LANGUAGE JAVASCRIPT
    AS 'throw "You must configure a Sundeck token before registering service  account.";';
EXCEPTION
    WHEN statement_error THEN
        let isalreadyef boolean := (select CONTAINS(:SQLERRM, 'API_INTEGRATION') AND CONTAINS(:SQLERRM, 'must be specified'));
        if (not isalreadyef) then
            RAISE;
        end if;
    WHEN OTHER THEN
        RAISE;
END;


create or replace function internal.wrapper_report_quota_used(request object)
    returns variant
    immutable
as
$$
    iff(length(internal.ef_report_quota_used(request):error) != 0,
        internal.throw_exception(internal.ef_report_quota_used(request):error),
        internal.ef_report_quota_used(request):failures)
$$;

create or replace function internal.report_quota_used(quota_used object)
    returns variant
as
$$
    internal.wrapper_report_quota_used(quota_used)
$$;

create function if not exists internal.get_ef_url()
    returns string
    language javascript
    as
    'throw "You must configure a Sundeck token to use this.";';

create function if not exists internal.get_tenant_url()
    returns string
    language javascript
    as
    'throw "You must configure a Sundeck token to use this.";';

create function if not exists internal.get_ef_token()
    returns string
    language javascript
    as
    'throw "You must configure a Sundeck token to use this.";';

create or replace procedure internal.setup_ef_url(url string) RETURNS STRING LANGUAGE SQL AS
BEGIN
    execute immediate 'create or replace function internal.get_ef_url() returns string as \'\\\'' || url || '\\\'\';';
END;


create or replace procedure internal.setup_sundeck_token(url string, token string) RETURNS STRING LANGUAGE SQL AS
BEGIN
    execute immediate 'create or replace function internal.get_ef_url() returns string as \'\\\'' || url || '\\\'\';';
    execute immediate 'create or replace function internal.get_ef_token() returns string as \'\\\'' || token || '\\\'\';';
END;

BEGIN
    create function if not exists internal.ef_register_tenant(request object)
        returns object
        language javascript
        as
        'throw "tenant register requires api gateway to be configured";';
EXCEPTION
    WHEN statement_error THEN
        let isalreadyef boolean := (select CONTAINS(:SQLERRM, 'API_INTEGRATION') AND CONTAINS(:SQLERRM, 'must be specified'));
        if (not isalreadyef) then
            RAISE;
        end if;
    WHEN OTHER THEN
        RAISE;
END;


create or replace function internal.wrapper_register_tenant(request object)
    returns object
    immutable
as
$$
    iff(length(internal.ef_register_tenant(request):error) != 0,
        internal.throw_exception(internal.ef_register_tenant(request):error),
        internal.ef_register_tenant(request))::object
$$;

create or replace function admin.register_tenant(sfAppName varchar, client_id varchar, client_secret varchar, dbname varchar)
    returns object
as
$$
    internal.wrapper_register_tenant(object_construct('sfAppName', sfAppName, 'clientKey', client_id, 'clientSecret', client_secret, 'nativeAppDatabase', dbname, 'externalId', internal.external_id()))
$$;


CREATE OR REPLACE PROCEDURE admin.setup_register_tenant_func() RETURNS STRING LANGUAGE SQL AS
BEGIN
    let url string := (select internal.get_ef_url());
    execute immediate '
        BEGIN
	        create or replace external function internal.ef_register_tenant(request object)
            returns object
            context_headers = (CURRENT_ACCOUNT, CURRENT_USER, CURRENT_ROLE, CURRENT_DATABASE, CURRENT_SCHEMA, CURRENT_REGION)
            api_integration = reference(\'opscenter_api_integration\')
            headers = ()
            as \'' || url || '/' || '/extfunc/register_tenant\';
        END;
    ';
END;


CREATE OR REPLACE PROCEDURE admin.setup_external_functions(api_integration_name string) RETURNS STRING LANGUAGE SQL AS
BEGIN
    let url string := (select internal.get_ef_url());
    let token string := (select internal.get_ef_token());
    execute immediate '
        BEGIN
            create or replace external function internal.ef_qlike(request object)
            returns object
            context_headers = (CURRENT_ACCOUNT, CURRENT_USER, CURRENT_ROLE, CURRENT_DATABASE, CURRENT_SCHEMA)
            api_integration = reference(\'' || api_integration_name || '\')
            headers = (\'sndk-token\' = \'' || token || '\')
            as \'' || url || '/extfunc/qlike\';

            create or replace external function internal.ef_notifications(request object)
            returns object
            context_headers = (CURRENT_ACCOUNT, CURRENT_USER, CURRENT_ROLE, CURRENT_DATABASE, CURRENT_SCHEMA)
            api_integration = reference(\'' || api_integration_name || '\')
            headers = (\'sndk-token\' = \'' || token || '\')
            as \'' || url || '/extfunc/notifications\';

            create or replace external function internal.ef_run(unused object, request object)
            returns object
            context_headers = (CURRENT_ACCOUNT, CURRENT_USER, CURRENT_ROLE, CURRENT_DATABASE, CURRENT_SCHEMA)
            api_integration = reference(\'' || api_integration_name || '\')
            headers = (\'sndk-token\' = \'' || token || '\')
            as \'' || url || '/extfunc/run\';

            create or replace external function internal.ef_report_quota_used(quota_used object)
            returns object
            context_headers = (CURRENT_ACCOUNT, CURRENT_USER, CURRENT_ROLE, CURRENT_DATABASE, CURRENT_SCHEMA)
            api_integration = reference(\'' || api_integration_name || '\')
            headers = (\'sndk-token\' = \'' || token || '\')
            as \'' || url || '/extfunc/update_usage_stats\';

            create or replace external function internal.ef_register_service_account(service_account object)
            returns object
            context_headers = (CURRENT_ACCOUNT, CURRENT_USER, CURRENT_ROLE, CURRENT_DATABASE, CURRENT_SCHEMA)
            api_integration = reference(\'' || api_integration_name || '\')
            headers = (\'sndk-token\' = \'' || token || '\')
            as \'' || url || '/extfunc/register_service_account\';
        END;
    ';

    MERGE INTO internal.config AS target
    USING (SELECT 'url' AS key, :url AS value
    ) AS source
    ON target.key = source.key
    WHEN MATCHED THEN
      UPDATE SET value = source.value
    WHEN NOT MATCHED THEN
      INSERT (key, value)
      VALUES (source.key, source.value);

    -- Start the user limits task (we know that Sundeck is linked)
    CALL internal.start_user_limits_task();
END;

CREATE OR REPLACE PROCEDURE admin.setup_sundeck_tenant_url(url string, token string) RETURNS STRING LANGUAGE SQL AS
BEGIN
    execute immediate 'create or replace function internal.get_tenant_url() returns string as \'\\\'' || url || '\\\'\';';
    execute immediate 'create or replace function internal.get_ef_token() returns string as \'\\\'' || token || '\\\'\';';

    MERGE INTO internal.config AS target
    USING (SELECT 'tenant_url' AS key, :url AS value
    ) AS source
    ON target.key = source.key
    WHEN MATCHED THEN
      UPDATE SET value = source.value
    WHEN NOT MATCHED THEN
      INSERT (key, value)
      VALUES (source.key, source.value);

    CALL admin.setup_external_functions('opscenter_api_integration');
END;

create or replace function internal.wrapper_register_service_account(request object)
    returns object
    immutable
as
$$
    iff(length(internal.ef_register_service_account(request):error) != 0,
        internal.throw_exception(internal.ef_register_service_account(request):error),
        internal.ef_register_service_account(request))::object
$$;

create or replace procedure admin.register_sundeck_account(username varchar, password varchar, role varchar, warehouse varchar)
returns object
language sql
execute as owner
as
begin
  let obj object := (select object_construct('username', :username, 'password', :password, 'warehouse', :warehouse, 'role', :role));
  let res object := (select internal.wrapper_register_service_account(:obj));


  return res;
end;

create or replace procedure admin.connect_sundeck(token text)
    returns object
    language sql
    execute as owner
as
DECLARE
    -- TODO should we check SYSTEM$GET_ALL_REFERENCES instead?
    integration_ref text default (select any_value(ref_or_alias) from internal.reference_management where ref_name = 'OPSCENTER_API_INTEGRATION');
    integration_name text default (select 'OPSCENTER_SUNDECK_EXTERNAL_FUNCTIONS');
    deployment text default (select internal.get_sundeck_deployment());
BEGIN
    -- Make sure OpsCenter has been opened and the API Integration permission has been granted (and created)
    if (integration_ref is null OR len(integration_ref) = 0) then
        return (select object_construct('error', 'OpsCenter is not configured to communicate with Sundeck. Please open the Native App in Snowflake and approve the permission request to create the API Integration.'));
    end if;

    -- Prevent collisions of api integration name
    if (deployment != 'prod') then
        integration_name := (select :integration_name || '_' || current_database());
    end if;

    -- Create the scalar UDF for the Sundeck auth token (EF URL set up by the app in permissions.py)
    execute immediate 'create or replace function internal.get_ef_token() returns string as \'\\\'' || token || '\\\'\';';

    -- Create all external functions that use the API Gateway and the auth token
    call admin.setup_external_functions('opscenter_api_integration');

    -- Success!
    return (select object_construct());
EXCEPTION
    WHEN OTHER THEN
        SYSTEM$LOG_ERROR(OBJECT_CONSTRUCT('error', 'Failed to connect to Sundeck. Please contact Sundeck for support.', 'SQLERRM', SQLERRM, 'SQLSTATE', SQLSTATE));
        return object_construct('error', 'Failed to connect to Sundeck. Please contact Sundeck for support.', 'SQLERRM', SQLERRM, 'SQLSTATE', SQLSTATE);
END;
