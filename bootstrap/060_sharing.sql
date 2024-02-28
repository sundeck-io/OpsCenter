

-- Access to the SUNDECK database is given to application_package in deploy.py
-- SHARING.GLOBAL_QUERY_HISTORY is already filtered on SNOWFLAKE_ACCOUNT_LOCATOR = CURRENT_ACCOUNT()
CREATE OR REPLACE VIEW REPORTING.SUNDECK_QUERY_HISTORY
   AS SELECT * from SHARING.GLOBAL_QUERY_HISTORY WHERE
     UPPER(SUNDECK_ACCOUNT_ID) = (select UPPER(value) from internal.config where key = 'tenant_id');
