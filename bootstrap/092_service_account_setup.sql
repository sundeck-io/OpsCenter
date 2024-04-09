
DROP PROCEDURE IF EXISTS admin.finalize_setup_from_service_account(varchar, varchar, varchar);
CREATE OR REPLACE PROCEDURE admin.finalize_setup_from_service_account(api_integration_ref_id varchar, url varchar, web_url varchar, token varchar default null)
RETURNS object
LANGUAGE sql
as
begin
    -- Create the task so we can run finalize_setup asynchronously (duplicated in finalize_setup)
    -- Does not start the task -- the first time the task runs, finalize_setup() will start the task.
    CREATE OR REPLACE TASK TASKS.UPGRADE_CHECK
        SCHEDULE = '1440 minute'
        ALLOW_OVERLAPPING_EXECUTION = FALSE
        USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = "XSMALL"
        AS
        CALL ADMIN.UPGRADE_CHECK();
    grant MONITOR, OPERATE on TASK TASKS.UPGRADE_CHECK to APPLICATION ROLE ADMIN;

    -- Merge all three properties into the config table in one merge statement.
    MERGE INTO internal.config AS target
    USING (
        SELECT $1 as key, $2 as value from VALUES(
            ('tenant_url', :web_url), ('url', :url), ('tenant_id', split_part(:web_url, '/', -1))
        )
    ) AS source
    ON target.key = source.key
    WHEN MATCHED THEN
        UPDATE SET value = source.value
    WHEN NOT MATCHED THEN
        INSERT (key, value) VALUES (source.key, source.value);

    -- Bind the given reference ID to the 'OPSCENTER_API_INTEGRATION' reference. Must match the reference in manifest.yml
    call admin.update_reference('OPSCENTER_API_INTEGRATION', 'ADD', :api_integration_ref_id);

    -- Save the token if provided
    let ret object;
    if (token is not null) then
        -- Create the scalar UDF for the Sundeck auth token (EF URL set up by the app in permissions.py)
        execute immediate 'create or replace function internal.get_ef_token() returns string as \'\\\'' || token || '\\\'\';';
        CALL admin.setup_external_functions('opscenter_api_integration');
    end if;
    return :ret;
end;

CREATE TABLE IF NOT EXISTS INTERNAL.TASK_UPGRADE_CHECK(run timestamp, success boolean, output variant);
CREATE OR REPLACE VIEW REPORTING.UPGRADE_CHECK_TASK_HISTORY AS SELECT * FROM INTERNAL.TASK_UPGRADE_CHECK;

CREATE OR REPLACE PROCEDURE admin.upgrade_check()
returns varchar
language sql
as
declare
    dt timestamp default current_timestamp();
begin
    let version varchar;
    call internal.get_config('post_setup') into :version;
    let setup_version varchar := (select internal.get_version());
    if (version is null or version <> setup_version) then
        call admin.finalize_setup();
    end if;

    insert into INTERNAL.TASK_UPGRADE_CHECK SELECT :dt, true,
        OBJECT_CONSTRUCT('last_version', :version, 'current_version', :setup_version)::VARIANT;
EXCEPTION
    WHEN OTHER THEN
        SYSTEM$LOG_ERROR(OBJECT_CONSTRUCT('error', 'Exception occurred during upgrade.', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate));
        insert into INTERNAL.TASK_UPGRADE_CHECK SELECT :dt, false,
            OBJECT_CONSTRUCT('SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate)::VARIANT;
        RAISE;
end;
