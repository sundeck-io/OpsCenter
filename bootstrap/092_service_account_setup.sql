
CREATE OR REPLACE PROCEDURE admin.finalize_setup_from_service_account(url varchar, web_url varchar)
RETURNS varchar
LANGUAGE sql
as
begin
    execute immediate 'create or replace function internal.get_ef_url() returns string as \'\\\'' || url || '\\\'\';';
    execute immediate 'create or replace function internal.get_tenant_url() returns string as \'\\\'' || web_url || '\\\'\';';

    call internal.set_config('tenant_url', web_url);
    call internal.set_config('url', url);
    call admin.update_reference('OPSCENTER_API_INTEGRATION', 'ADD', SYSTEM$REFERENCE('API Integration', 'OPSCENTER_API_INTEGRATION', 'persistent', 'usage'));
end;

CREATE OR REPLACE PROCEDURE admin.upgrade_check()
returns varchar
language sql
as
begin
    let version varchar;
    call internal.get_config('post_setup') into :version;
    let setup_version varchar := (select internal.setup_version());
    if (version is null or version <> setup_version) then
        call admin.finalize_setup();
    end if;

end;
