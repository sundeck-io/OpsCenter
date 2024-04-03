
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


-- Add query signature helper to Sundeck
BEGIN
    CREATE FUNCTION IF NOT EXISTS internal.ef_has_signature(request object)
        RETURNS VARIANT
        LANGUAGE JAVASCRIPT
    AS 'throw "You must configure a Sundeck token before using has_signature.";';
EXCEPTION
    WHEN statement_error THEN
        let isalreadyef boolean := (select CONTAINS(:SQLERRM, 'API_INTEGRATION') AND CONTAINS(:SQLERRM, 'must be specified'));
        if (not isalreadyef) then
            RAISE;
        end if;
    WHEN OTHER THEN
        RAISE;
END;


create or replace function internal.wrapper_has_signature(request object)
    returns boolean
    immutable
as
$$
    iff(length(internal.ef_has_signature(request):error) != 0,
        internal.throw_exception(internal.ef_has_signature(request):error),
        internal.ef_has_signature(request):has_query_signature)::boolean
$$;


BEGIN
    CREATE FUNCTION IF NOT EXISTS internal.ef_signatures_match(request object)
        RETURNS VARIANT
        LANGUAGE JAVASCRIPT
    AS 'throw "You must configure a Sundeck token before using signatures_match.";';
EXCEPTION
    WHEN statement_error THEN
        let isalreadyef boolean := (select CONTAINS(:SQLERRM, 'API_INTEGRATION') AND CONTAINS(:SQLERRM, 'must be specified'));
        if (not isalreadyef) then
            RAISE;
        end if;
    WHEN OTHER THEN
        RAISE;
END;


create or replace function internal.wrapper_signatures_match(request object)
    returns boolean
    immutable
as
$$
    iff(length(internal.ef_signatures_match(request):error) != 0,
        internal.throw_exception(internal.ef_signatures_match(request):error),
        internal.ef_signatures_match(request):has_signature_matched)::boolean
$$;


BEGIN
    CREATE FUNCTION IF NOT EXISTS internal.ef_signature_target(request object)
        RETURNS VARIANT
        LANGUAGE JAVASCRIPT
    AS 'throw "You must configure a Sundeck token before using signature_target.";';
EXCEPTION
    WHEN statement_error THEN
        let isalreadyef boolean := (select CONTAINS(:SQLERRM, 'API_INTEGRATION') AND CONTAINS(:SQLERRM, 'must be specified'));
        if (not isalreadyef) then
            RAISE;
        end if;
    WHEN OTHER THEN
        RAISE;
END;


create or replace function internal.wrapper_signature_target(request object)
    returns variant
    immutable
as
$$
    iff(length(internal.ef_signature_target(request):error) != 0,
        internal.throw_exception(internal.ef_signature_target(request):error),
        internal.ef_signature_target(request):target_warehouse)::variant
$$;

-- Add routine helper to Sundeck
BEGIN
    CREATE FUNCTION IF NOT EXISTS internal.ef_list_routines(request object)
        RETURNS VARIANT
        LANGUAGE JAVASCRIPT
    AS 'throw "You must configure a Sundeck token before using list_routines.";';
EXCEPTION
    WHEN statement_error THEN
        let isalreadyef boolean := (select CONTAINS(:SQLERRM, 'API_INTEGRATION') AND CONTAINS(:SQLERRM, 'must be specified'));
        if (not isalreadyef) then
            RAISE;
        end if;
    WHEN OTHER THEN
        RAISE;
END;

create or replace function internal.wrapper_list_routines(request object)
    returns VARIANT
    immutable
as
$$
    iff(length(internal.ef_list_routines(request):error) != 0,
        internal.throw_exception(internal.ef_list_routines(request):error),
        internal.ef_list_routines(request):routines)
$$;

-- Add broker helper to Sundeck
BEGIN
    CREATE FUNCTION IF NOT EXISTS internal.ef_list_brokers(request object)
        RETURNS VARIANT
        LANGUAGE JAVASCRIPT
    AS 'throw "You must configure a Sundeck token before using list_brokers.";';
EXCEPTION
    WHEN statement_error THEN
        let isalreadyef boolean := (select CONTAINS(:SQLERRM, 'API_INTEGRATION') AND CONTAINS(:SQLERRM, 'must be specified'));
        if (not isalreadyef) then
            RAISE;
        end if;
    WHEN OTHER THEN
        RAISE;
END;

create or replace function internal.wrapper_list_brokers(request object)
    returns VARIANT
    immutable
as
$$
    iff(length(internal.ef_list_brokers(request):error) != 0,
        internal.throw_exception(internal.ef_list_brokers(request):error),
        internal.ef_list_brokers(request):brokers)
$$;

BEGIN
    CREATE FUNCTION IF NOT EXISTS internal.ef_verify_token(request object)
    RETURNS VARIANT
    LANGUAGE JAVASCRIPT
    AS 'throw "You must configure a Sundeck token before using verify_token.";';
EXCEPTION
    when statement_error then
        let isalreadyef boolean := (select CONTAINS(:SQLERRM, 'API_INTEGRATION') AND CONTAINS(:SQLERRM, 'must be specified'));
        if (not isalreadyef) then
            RAISE;
        end if;
    WHEN OTHER THEN
        RAISE;
END;

create or replace function internal.wrapper_verify_token(request object)
    returns TEXT
    immutable
as
$$
    iff(length(internal.ef_verify_token(request):error) != 0,
        internal.throw_exception(internal.ef_verify_token(request):error),
        -- returns NULL for success
        NULL)::TEXT
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
DECLARE
    MISSING_REFERENCE EXCEPTION(-20500, 'API Integration reference not found. Please complete the Native App linking in Sundeck and call the ADMIN.FINALIZE_SETUP() procedure.');
BEGIN
    let hasUrl varchar;
    call internal.get_config('url') into :hasUrl;
    if (:hasUrl is null) then
        return 'You must configure a Sundeck token to use this.';
    end if;
    -- Make sure we have the reference before we try to create the external functions
    let has_ref boolean := (select ARRAY_SIZE(PARSE_JSON(system$get_all_references('OPSCENTER_API_INTEGRATION'))) > 0);
    if (not :has_ref) then
        raise MISSING_REFERENCE;
    end if;
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

            create or replace external function internal.ef_has_signature(sig_params object)
            returns object
            context_headers = (CURRENT_ACCOUNT, CURRENT_USER, CURRENT_ROLE, CURRENT_DATABASE, CURRENT_SCHEMA)
            api_integration = reference(\'' || api_integration_name || '\')
            headers = (\'sndk-token\' = \'' || token || '\')
            as \'' || url || '/extfunc/has_signature\';

            create or replace external function internal.ef_signatures_match(sig_params object)
            returns object
            context_headers = (CURRENT_ACCOUNT, CURRENT_USER, CURRENT_ROLE, CURRENT_DATABASE, CURRENT_SCHEMA)
            api_integration = reference(\'' || api_integration_name || '\')
            headers = (\'sndk-token\' = \'' || token || '\')
            as \'' || url || '/extfunc/signatures_match\';


            create or replace external function internal.ef_signature_target(sig_params object)
            returns object
            context_headers = (CURRENT_ACCOUNT, CURRENT_USER, CURRENT_ROLE, CURRENT_DATABASE, CURRENT_SCHEMA)
            api_integration = reference(\'' || api_integration_name || '\')
            headers = (\'sndk-token\' = \'' || token || '\')
            as \'' || url || '/extfunc/signature_target\';

            create or replace external function internal.ef_list_routines(params object)
            returns object
            context_headers = (CURRENT_ACCOUNT, CURRENT_USER, CURRENT_ROLE, CURRENT_DATABASE, CURRENT_SCHEMA)
            api_integration = reference(\'' || api_integration_name || '\')
            headers = (\'sndk-token\' = \'' || token || '\')
            as \'' || url || '/extfunc/list_routines\';

            create or replace external function internal.ef_list_brokers(params object)
            returns object
            context_headers = (CURRENT_ACCOUNT, CURRENT_USER, CURRENT_ROLE, CURRENT_DATABASE, CURRENT_SCHEMA)
            api_integration = reference(\'' || api_integration_name || '\')
            headers = (\'sndk-token\' = \'' || token || '\')
            as \'' || url || '/extfunc/list_brokers\';

            create or replace external function internal.ef_verify_token(params object)
            returns object
            context_headers = (CURRENT_ACCOUNT, CURRENT_USER, CURRENT_ROLE, CURRENT_DATABASE, CURRENT_SCHEMA)
            api_integration = reference(\'' || api_integration_name || '\')
            headers = (\'sndk-token\' = \'' || token || '\')
            as \'' || url || '/extfunc/verify_token\';

        END;
    ';

END;

CREATE OR REPLACE PROCEDURE admin.setup_sundeck_tenant_url(url string, token string) RETURNS STRING LANGUAGE SQL AS
BEGIN
    execute immediate 'create or replace function internal.get_ef_token() returns string as \'\\\'' || token || '\\\'\';';
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
BEGIN
    -- Create the scalar UDF for the Sundeck auth token (EF URL set up by the app in permissions.py)
    execute immediate 'create or replace function internal.get_ef_token() returns string as \'\\\'' || token || '\\\'\';';

    -- Success!
    return object_construct();
END;
