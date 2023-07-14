
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

create or replace function tools.qlike(query_text varchar, selector varchar, database varchar, currentschema varchar)
    returns boolean
as
$$
    internal.wrapper_qlike(object_construct('selector', selector, 'query_text', query_text, 'database', database, 'schema', currentschema))
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


create function if not exists internal.get_ef_url()
    returns string
    language javascript
    as
    'throw "You must configure a Sundeck token to use this.";';

create function if not exists internal.get_ef_token()
    returns string
    language javascript
    as
    'throw "You must configure a Sundeck token to use this.";';


create or replace procedure internal.setup_sundeck_token(url string, token string) RETURNS STRING LANGUAGE SQL AS
BEGIN
    execute immediate 'create or replace function internal.get_ef_url() returns string as \'\\\'' || url || '\\\'\';';
    execute immediate 'create or replace function internal.get_ef_token() returns string as \'\\\'' || token || '\\\'\';';
END;


CREATE OR REPLACE PROCEDURE admin.setup_external_functions(api_integration_ref string) RETURNS STRING LANGUAGE SQL AS
BEGIN
    let url string := (select internal.get_ef_url());
    let token string := (select internal.get_ef_token());
    execute immediate '
        BEGIN
            create or replace external function internal.ef_qlike(request object)
            returns object
            context_headers = (CURRENT_ACCOUNT, CURRENT_USER, CURRENT_ROLE, CURRENT_DATABASE, CURRENT_SCHEMA)
            api_integration = ' || api_integration_ref || '
            headers = (\'sndk-token\' = \'sndk_' || token || '\')
            as \'' || url || '/extfunc/qlike\';

            create or replace external function internal.ef_notifications(request object)
            returns object
            context_headers = (CURRENT_ACCOUNT, CURRENT_USER, CURRENT_ROLE, CURRENT_DATABASE, CURRENT_SCHEMA)
            api_integration = ' || api_integration_ref || '
            headers = (\'sndk-token\' = \'sndk_' || token || '\')
            as \'' || url || '/extfunc/notifications\';

            create or replace external function internal.ef_run(unused object, request object)
            returns object
            context_headers = (CURRENT_ACCOUNT, CURRENT_USER, CURRENT_ROLE, CURRENT_DATABASE, CURRENT_SCHEMA)
            api_integration = ' || api_integration_ref || '
            headers = (\'sndk-token\' = \'sndk_' || token || '\')
            as \'' || url || '/extfunc/run\';
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
END;

CREATE OR REPLACE PROCEDURE admin.setup_external_functions() RETURNS STRING LANGUAGE SQL AS
BEGIN
    call admin.setup_external_functions('reference(\'opscenter_api_integration\')');
END;
