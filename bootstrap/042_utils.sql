
create or replace procedure internal.enable_query_hash()
returns boolean
as
begin
    let enabled boolean := (SELECT SYSTEM$BEHAVIOR_CHANGE_BUNDLE_STATUS('2023_06') = 'ENABLED');
    if (:enabled) then
       execute immediate $$
    begin
create or replace function tools.is_reoccurring_query(qph varchar, size number)
returns boolean
immutable
as
'qph in (select query_parameterized_hash from reporting.enriched_query_history group by query_parameterized_hash having count(query_parameterized_hash) > size)'
;

create or replace function tools.is_ad_hoc_query(qph varchar, size number)
returns boolean
immutable
as
'qph in (select query_parameterized_hash from reporting.enriched_query_history group by query_parameterized_hash having count(query_parameterized_hash) < size)'
;
    end;
    $$;
    end if;
end;

create function if not exists tools.is_reoccurring_query(qph varchar, size number)
returns boolean
as
'false';

create function if not exists tools.is_ad_hoc_query(qph varchar, size number)
returns boolean
as
'false';

create or replace function tools.has_signature(query_db varchar, query_schema varchar, query_text varchar)
    returns boolean
as
$$
    internal.wrapper_has_signature(object_construct('database', query_db, 'schema', query_schema, 'query_text', query_text))
$$;

create or replace function tools.signatures_match(db_first varchar, schema_first varchar, query_text_first varchar,
            db_second varchar, schema_second varchar, query_text_second varchar)
    returns boolean
as
$$
    internal.wrapper_signatures_match(object_construct('database_first', db_first, 'schema_first', schema_first, 'query_text_first', query_text_first,
    'database_second', db_second, 'schema_second', schema_second, 'query_text_second', query_text_second))
$$;

create or replace function tools.signature_target(query_db varchar, query_schema varchar, query_text varchar,
        pin_table_dbname varchar, pin_table_schemaname varchar, pin_table_tablename varchar))
    returns variant
as
$$
    internal.wrapper_signature_target(object_construct('database', query_db, 'schema', query_schema, 'query_text', query_text,
    'pin_table', object_construct('database', pin_table_dbname, 'schema', pin_table_schemaname, 'table', pin_table_tablename)))
$$
