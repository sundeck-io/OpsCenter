
create table if not exists internal.quota_task_history(start_time timestamp, end_time timestamp, credits_used object, outcome string);
CREATE OR REPLACE VIEW REPORTING.QUOTA_TASK_HISTORY AS SELECT * FROM INTERNAL.QUOTA_TASK_HISTORY;

-- returns SQL which reads the query_history table function and aggregates the credits used by both user and role. executed within a task
-- as an owner's rights procedure cannot call the query_history table function.
create or replace procedure internal.get_recent_quota_usage_select()
returns string
language sql
as
BEGIN
    let s string := $$
-- Compute just the remainder since the last time query history maintenance ran
with todays_queries as(
    select
        total_elapsed_time,
        credits_used_cloud_services,
        warehouse_size,
        user_name,
        role_name
    from table(information_schema.query_history(
        RESULT_LIMIT => 10000,
        END_TIME_RANGE_START => TO_TIMESTAMP_LTZ(INTERNAL.GET_QUERY_HISTORY_FUNC_START_RANGE(current_timestamp())),
        END_TIME_RANGE_END => current_timestamp()))
),
costed_queries as (
    select
        greatest(0,total_elapsed_time) * internal.warehouse_credits_per_milli(warehouse_size) + credits_used_cloud_services as credits_used,
        user_name,
        role_name
    from todays_queries
),
usage as (
    select user_name as name, sum(credits_used) as credits_used, 'user' as persona
    from costed_queries
    group by user_name
    union all
    select role_name as name, sum(credits_used) as credits_used, 'role' as persona
    from costed_queries
    group by role_name
)
select name, persona, credits_used from usage;
$$;
    return s;
END;

create table if not exists internal.aggregated_hourly_quota(hour_of_day integer, name string, persona string, credits_used float, last_updated timestamp);

-- account_usage.query_history has a 45 minute delay, we want to get the last hour bucket which is "guaranteed" to be
-- complete. We have to rewind one extra hour backward to make sure that whole hour is reflected in the query_history view.
-- e.g. 1730 - 45mins = 1645. However, the '16' bucket cannot be complete because the range [1645,1700) is not guaranteed
--   to be complete per snowflake. However, the '15' bucket can be safely computed.
CREATE OR REPLACE FUNCTION INTERNAL.GET_QUERY_HISTORY_START_RANGE(t TIMESTAMP)
RETURNS TIMESTAMP
AS
$$
    timestampadd('hour', -1, date_trunc('hour', timestampadd('minutes', -45, t)))
$$;

CREATE OR REPLACE FUNCTION INTERNAL.GET_QUERY_HISTORY_FUNC_START_RANGE(t TIMESTAMP)
RETURNS TIMESTAMP
AS
$$
    date_trunc('hour', timestampadd('minutes', -45, t))
$$;

CREATE OR REPLACE FUNCTION INTERNAL.MAKE_QUOTA_HOUR_BUCKET(t TIMESTAMP)
RETURNS INTEGER
AS
$$
    extract(HOUR from internal.get_query_history_start_range(t))
$$;


CREATE OR REPLACE PROCEDURE INTERNAL.REFRESH_HOURLY_QUERY_USAGE(now TIMESTAMP)
    RETURNS STRING
    LANGUAGE SQL
AS
$$
DECLARE
    start_time timestamp default current_timestamp();
BEGIN
    BEGIN TRANSACTION;
    -- Get hour bucket (0-23) that we're going to update
    let qh_range_start timestamp := (select get_query_history_start_range(:now));
    let hour_bucket integer := (select make_quota_hour_bucket(:now));

    -- Delete rows for that hour bucket already in the table
    delete from aggregated_hourly_quota where hour_of_day = :hour_bucket;

    -- Read account_usage.query_history and summarize by user/role and write to the table
    insert into aggregated_hourly_quota(hour_of_day, name, persona, credits_used, last_updated)
    with todays_queries as(
        SELECT
            total_elapsed_time,
            credits_used_cloud_services,
            warehouse_size,
            user_name,
            role_name
        FROM
            snowflake.account_usage.query_history
        WHERE
            END_TIME BETWEEN :qh_range_start AND timestampadd('hour', 1, :qh_range_start)
    ),
    costed_queries as (
        select
            greatest(0,total_elapsed_time) * internal.warehouse_credits_per_milli(warehouse_size) + credits_used_cloud_services as credits_used,
            user_name,
            role_name
        from todays_queries
    ),
    usage as (
        select user_name as name, sum(credits_used) as credits_used, 'user' as persona
        from costed_queries
        group by user_name
        union all
        select role_name as name, sum(credits_used) as credits_used, 'role' as persona
        from costed_queries
        group by role_name
    )
    select :hour_bucket, name, persona, credits_used, current_timestamp() from usage;

    -- Record the number of rows we wrote
    let rows_added integer := SQLROWCOUNT;

    let outcome string := 'Added ' || :rows_added || ' rows to aggregated_hourly_quota for the hour bucket ' || :hour_bucket || ', starting at ' || :qh_range_start;

    -- Record the success
    insert into INTERNAL.QUOTA_TASK_HISTORY SELECT :start_time, current_timestamp(), NULL, :outcome;

    COMMIT;

    return outcome;
EXCEPTION
    WHEN OTHER THEN
        SYSTEM$LOG_ERROR(OBJECT_CONSTRUCT('error', 'Exception occurred while updating hourly quota aggregation.', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate));
        ROLLBACK;
        insert into INTERNAL.QUOTA_TASK_HISTORY SELECT :start_time, current_timestamp(), NULL, 'failed to run hourly_quota_actions: ' || :sqlerrm;
        RAISE;
END;
$$;

-- Cleanup old artifacts, accepting that a task may fail once if it happens to run
-- in between the drop and finalize_setup() procedure's completion.
drop procedure if exists internal.get_daily_quota_select();
