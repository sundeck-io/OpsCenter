BEGIN
    CREATE SCHEMA IF NOT EXISTS "{DATABASE}".SHARING;
    CREATE OR REPLACE TABLE "{DATABASE}".SHARING.GLOBAL_QUERY_HISTORY(
        SNOWFLAKE_QUERY_ID text,
        SUNDECK_QUERY_ID text,
        BROKER_NAME text,
        QUERY_TEXT_RECEIVED text,
        QUERY_TEXT_FINAL text,
        RAW_SNOWFLAKE_SUBMISSION_TIME timestamp_ntz,
        RAW_SNOWFLAKE_END_TIME timestamp_ntz,
        UPDATED_WAREHOUSE_TARGET text,
        SUNDECK_STATUS text,
        SUNDECK_ERROR_CODE text,
        SUNDECK_ERROR_MESSAGE text,
        RAW_START_TIME timestamp_ntz,
        SUNDECK_ACCOUNT_ID text,
        USER_NAME text,
        ROLE_NAME text,
        STARTING_WAREHOUSE text,
        WORKLOAD_TAG text,
        SNOWFLAKE_SESSION_ID text,
        RAW_ACTIONS variant
    );
END;
