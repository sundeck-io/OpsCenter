
create or replace procedure internal.enable_query_hash()
returns boolean
as
begin
    let enabled boolean := (SELECT SYSTEM$BEHAVIOR_CHANGE_BUNDLE_STATUS('2023_06') = 'ENABLED');
    if (:enabled) then
       execute immediate $$
    begin
create or replace function tools.is_repeated_query(qph varchar, size number)
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
else
    execute immediate $$
    begin
create or replace function tools.is_repeated_query(qph varchar, size number)
returns boolean
as
'false';
create or replace function tools.is_ad_hoc_query(qph varchar, size number)
returns boolean
as
'false';
    end;
    $$;
end if;
end;

call internal.enable_query_hash();
