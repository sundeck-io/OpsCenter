
-- Create a procedure to enable Sundeck developers to debug their own installations of OpsCenter. No-op for non-Sundeck snowflake accounts.
BEGIN
    let same_org boolean := (select current_organization_name() = 'SUNDECK');
    IF (same_org) THEN
        SYSTEM$LOG_INFO('Creating run_as_app procedure because we are installing the application from the same organization');
        CREATE OR REPLACE PROCEDURE admin.run_as_app(sql string)
        RETURNS table()
        language sql
        execute as owner
        AS
        BEGIN
            let rs resultset := (execute immediate sql);
            return table(rs);
        END;
        -- usage granted in 100_final_perms
    ELSE
        SYSTEM$LOG_INFO('Skipping run_as_app procedure because we are installing the application from a different organization');
    END IF;
EXCEPTION
    WHEN OTHER THEN
        SYSTEM$LOG_ERROR(OBJECT_CONSTRUCT('error', 'Failed to create run_as_app procedure.', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate));
END;
