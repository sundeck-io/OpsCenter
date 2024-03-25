BEGIN
    CREATE SCHEMA IF NOT EXISTS "{DATABASE}".SHARING;
    CREATE OR REPLACE TABLE "{DATABASE}".SHARING.GLOBAL_QUERY_HISTORY(
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
        SUNDECK_START_TIME timestamp_ltz,
        SUNDECK_ACCOUNT_ID text,
        USER_NAME text,
        ROLE_NAME text,
        ACTIONS_EXECUTED variant,
        SCHEMA_ONLY_REQUEST boolean,
        STARTING_WAREHOUSE text,
        WORKLOAD_TAG text,
        SNOWFLAKE_SESSION_ID text
    );
END;
