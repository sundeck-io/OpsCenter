BEGIN
    CREATE SCHEMA IF NOT EXISTS "{APPLICATION_PACKAGE}".SHARING;

    -- Must be kept in sync with the SUNDECK database
    CREATE OR REPLACE TABLE "{APPLICATION_PACKAGE}".SHARING.EMPTY_QUERY_HISTORY(
        SNOWFLAKE_ACCOUNT_LOCATOR text,
        SNOWFLAKE_QUERY_ID text,
        SUNDECK_QUERY_ID text,
        BROKER_NAME text,
        QUERY_TEXT_RECEIVED text,
        QUERY_TEXT_FINAL text,
        SNOWFLAKE_SUBMISSION_TIME timestamp_ltz,
        SNOWFLAKE_END_TIME timestamp_ltz,
        UPDATED_WAREHOUSE_TARGET text,
        SUNDECK_STATUS text,
        SUNDECK_ERROR_CODE text,
        SUNDECK_ERROR_MESSAGE text,
        USER_NAME text,
        ROLE_NAME text,
        SNOWFLAKE_REGION text,
        SNOWFLAKE_CLOUD text,
        SUNDECK_START_TIME timestamp_ltz,
        SUNDECK_ACCOUNT_ID text,
        ACTIONS_EXECUTED variant,
        SCHEMA_ONLY_REQUEST boolean);

    -- Creates an empty view with the schema we expect.
    CREATE OR REPLACE VIEW "{APPLICATION_PACKAGE}".SHARING.GLOBAL_QUERY_HISTORY AS
        SELECT * EXCLUDE(SNOWFLAKE_ACCOUNT_LOCATOR, SNOWFLAKE_CLOUD, SNOWFLAKE_REGION) FROM "{APPLICATION_PACKAGE}".SHARING.EMPTY_QUERY_HISTORY;

    -- Grant access on the view to the application package.
    GRANT USAGE ON SCHEMA "{APPLICATION_PACKAGE}".SHARING TO SHARE IN APPLICATION PACKAGE "{APPLICATION_PACKAGE}";
    GRANT SELECT ON VIEW "{APPLICATION_PACKAGE}".SHARING.GLOBAL_QUERY_HISTORY TO SHARE IN APPLICATION PACKAGE "{APPLICATION_PACKAGE}";
END;
