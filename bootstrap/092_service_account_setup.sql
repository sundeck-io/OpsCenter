
CREATE OR REPLACE PROCEDURE admin.finalize_setup_from_service_account(api_integration_ref_id varchar, url varchar, web_url varchar)
RETURNS varchar
LANGUAGE sql
as
begin
    execute immediate 'create or replace function internal.get_ef_url() returns string as \'\\\'' || url || '\\\'\';';
    execute immediate 'create or replace function internal.get_tenant_url() returns string as \'\\\'' || web_url || '\\\'\';';

    -- Create the task so we can run finalize_setup asynchronously
    CREATE OR REPLACE TASK TASKS.UPGRADE_CHECK
        SCHEDULE = '1440 minute'
        ALLOW_OVERLAPPING_EXECUTION = FALSE
        USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = "XSMALL"
        AS
        CALL ADMIN.UPGRADE_CHECK();

    call internal.set_config('tenant_url', :web_url);
    call internal.set_config('url', :url);

    // Bind the given reference ID to the 'OPSCENTER_API_INTEGRATION' reference. Must match the reference in manifest.yml
    let ref_name text := (select 'OPSCENTER_API_INTEGRATION');

    // We can't use admin.update_reference because it expects that the OpsCenter token is already set.
    select system$set_reference(:ref_name, :api_integration_ref_id);
    insert into internal.reference_management (ref_name, operation, ref_or_alias) values (:ref_name, 'finalize_setup_from_service_account', :api_integration_ref_id);
end;

CREATE OR REPLACE PROCEDURE admin.upgrade_check()
returns varchar
language sql
as
begin
    let version varchar;
    call internal.get_config('post_setup') into :version;
    let setup_version varchar := (select internal.get_version());
    if (version is null or version <> setup_version) then
        call admin.finalize_setup();
    end if;

end;
