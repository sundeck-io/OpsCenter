BEGIN
    CREATE SCHEMA IF NOT EXISTS "{APPLICATION_PACKAGE}".SHARING;

    GRANT REFERENCE_USAGE ON DATABASE "{SUNDECK_DB}" TO SHARE IN APPLICATION PACKAGE "{APPLICATION_PACKAGE}";

    -- Create a table in the application package that contains the mapping of snowflake regions to cloud and region.
    SHOW REGIONS;
    CREATE OR REPLACE TABLE "{APPLICATION_PACKAGE}".SHARING.REGIONS(snowflake_region text, cloud text, region text) AS
        SELECT "snowflake_region", "cloud", "region" from table(result_scan(last_query_id()));

    -- Create a view in the application package that is filtered to CURRENT_ACCOUNT(). It is critical that the
    -- filtering is done here to ensure a user only sees their own history.
    -- Correct some wrongly-generated azure regions.
    CREATE OR REPLACE VIEW "{APPLICATION_PACKAGE}".SHARING.GLOBAL_QUERY_HISTORY AS
        SELECT
            SNOWFLAKE_QUERY_ID,
            SUNDECK_QUERY_ID,
            FLOW_NAME, -- TODO drop from app-package view once released
            FLOW_NAME as BROKER_NAME, -- TODO use BROKER_NAME once Sundeck is updated
            QUERY_TEXT_RECEIVED,
            QUERY_TEXT_FINAL,
            ALT_WAREHOUSE_ROUTE, -- TODO drop from app-package view once released
            ALT_WAREHOUSE_ROUTE as UPDATED_WAREHOUSE_TARGET, -- TODO use UPDATED_WAREHOUSE_TARGET once Sundeck is updated
            SUNDECK_STATUS,
            SUNDECK_ERROR_CODE,
            SUNDECK_ERROR_MESSAGE,
            USER_NAME,
            ROLE_NAME,
            SUNDECK_START_TIME, -- TODO drop after "Do data transforms after secure view (#605)" is released
            RAW_START_TIME,
            SNOWFLAKE_SUBMISSION_TIME, -- TODO drop after "Do data transforms after secure view (#605)" is released
            RAW_SNOWFLAKE_SUBMISSION_TIME,
            SNOWFLAKE_END_TIME, -- TODO drop after "Do data transforms after secure view (#605)" is released
            RAW_SNOWFLAKE_END_TIME,
            SUNDECK_ACCOUNT_ID,
            ACTIONS_EXECUTED, -- TODO drop after "Do data transforms after secure view (#605)" is released
            SCHEMA_ONLY_REQUEST, -- TODO drop after "Do data transforms after secure view (#605)" is released
            STARTING_WAREHOUSE,
            WORKLOAD_TAG,
            SNOWFLAKE_SESSION_ID,
            RAW_ACTIONS
        FROM "{SUNDECK_DB}".INTERNAL.GLOBAL_QUERY_HISTORY
            WHERE UPPER(SNOWFLAKE_ACCOUNT_LOCATOR) = UPPER(CURRENT_ACCOUNT()) and
            IFF(UPPER(SNOWFLAKE_CLOUD) = 'AZURE', REPLACE(UPPER(SNOWFLAKE_REGION), '-', ''), UPPER(SNOWFLAKE_REGION)) =
                (SELECT UPPER(region) from "{APPLICATION_PACKAGE}".SHARING.REGIONS WHERE snowflake_region = SPLIT_PART(CURRENT_REGION(), '.', -1)) and
            UPPER(SNOWFLAKE_CLOUD) = (SELECT UPPER(cloud) from "{APPLICATION_PACKAGE}".SHARING.REGIONS WHERE snowflake_region = SPLIT_PART(CURRENT_REGION(), '.', -1));

    -- Grant access on the view to the application package.
    GRANT USAGE ON SCHEMA "{APPLICATION_PACKAGE}".SHARING TO SHARE IN APPLICATION PACKAGE "{APPLICATION_PACKAGE}";
    GRANT SELECT ON VIEW "{APPLICATION_PACKAGE}".SHARING.GLOBAL_QUERY_HISTORY TO SHARE IN APPLICATION PACKAGE "{APPLICATION_PACKAGE}";
    GRANT SELECT ON TABLE "{APPLICATION_PACKAGE}".SHARING.REGIONS TO SHARE IN APPLICATION PACKAGE "{APPLICATION_PACKAGE}";
END;
