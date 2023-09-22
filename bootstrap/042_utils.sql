
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
