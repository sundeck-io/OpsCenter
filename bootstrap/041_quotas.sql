
create table if not exists internal.quota_task_history(start_time timestamp, end_time timestamp, credits_used object, outcome string);
CREATE OR REPLACE VIEW REPORTING.QUOTA_TASK_HISTORY AS SELECT * FROM INTERNAL.QUOTA_TASK_HISTORY;

create table if not exists internal.aggregated_hourly_quota(day date, hour_of_day integer, name string, persona string, credits_used float, last_updated timestamp);

-- We want to read the last 1 hour plus N minutes where N is the number of minutes since top of the hour.
-- This has the limitation that if there are more than 10K queries run in a two-hour window, we will miss some.
CREATE OR REPLACE FUNCTION INTERNAL.GET_QUERY_HISTORY_FUNC_START_RANGE(t TIMESTAMP)
RETURNS TIMESTAMP
AS
$$
    timestampadd('hour', -1, date_trunc('hour', t))
$$;


CREATE OR REPLACE PROCEDURE INTERNAL.REFRESH_HOURLY_QUERY_USAGE_SQL(now TIMESTAMP)
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
    let sql string := $$
DECLARE
    start_time timestamp default current_timestamp();
    qh_func_start_time timestamp default internal.get_query_history_func_start_range('$$ || :now || $$');
BEGIN
    BEGIN TRANSACTION;

    -- Read account_usage.query_history and summarize by user/role and write to the table
    MERGE INTO internal.aggregated_hourly_quota q
    USING (
        with todays_queries as (
            select total_elapsed_time,
                   credits_used_cloud_services,
                   warehouse_size,
                   user_name,
                   role_name,
                   end_time
            from table (information_schema.query_history(
                   RESULT_LIMIT => 10000,
                   -- TODO the function demands TIMESTAMP_LTZ. What does that actually do to our TIMESTAMP_NTZ?
                   END_TIME_RANGE_START => TO_TIMESTAMP_LTZ(:qh_func_start_time),
                   END_TIME_RANGE_END => current_timestamp()))
        ), costed_queries as (
            select
                greatest(0, total_elapsed_time) * internal.warehouse_credits_per_milli(warehouse_size) +
                credits_used_cloud_services as credits_used,
                user_name,
                role_name,
                end_time
            from todays_queries
        ), usage as (
            select
                user_name                   as name,
                sum(credits_used)           as credits_used,
                date_trunc('day', end_time) as day,
                extract(HOUR from end_time) as hour_of_day,
                'user'                      as persona
            from costed_queries
            group by user_name, day, hour_of_day
            union all
            select role_name                   as name,
                sum(credits_used)           as credits_used,
                date_trunc('day', end_time) as day,
                extract(HOUR from end_time) as hour_of_day,
                'role'                      as persona
            from costed_queries
            group by role_name, day, hour_of_day
        ) select name, credits_used, day, hour_of_day, persona from usage
    ) new_usage (name, credits_used, day, hour_of_day, persona)
    -- Name + persona + date + hour, e.g. (josh, user, 2023-08-24, 12)
    ON q.name = new_usage.name AND q.persona = new_usage.persona AND q.day = new_usage.day AND q.hour_of_day = new_usage.hour_of_day
    WHEN MATCHED THEN
        -- Set the new credits_used as we recomputed that whole hour
        UPDATE SET q.credits_used = new_usage.credits_used, q.last_updated = current_timestamp()
    WHEN NOT MATCHED THEN
        INSERT (name, credits_used, day, hour_of_day, persona, last_updated)
        VALUES(new_usage.name, new_usage.credits_used, new_usage.day, new_usage.hour_of_day, new_usage.persona, current_timestamp());

    -- Record the number of rows we wrote
    let rows_added integer := SQLROWCOUNT;

    let outcome string := 'Updated ' || :rows_added || ' rows in aggregated_hourly_quota since ' || :qh_func_start_time;

    -- Record the success
    insert into INTERNAL.QUOTA_TASK_HISTORY SELECT :start_time, current_timestamp(), NULL, :outcome;

    COMMIT;

    return :outcome;
EXCEPTION
    WHEN OTHER THEN
        SYSTEM$LOG_ERROR(OBJECT_CONSTRUCT('error', 'Exception occurred while updating hourly quota aggregation.', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate));
        ROLLBACK;
        insert into INTERNAL.QUOTA_TASK_HISTORY SELECT :start_time, current_timestamp(), NULL, 'failed to run hourly_quota_actions: ' || :sqlerrm;
        RAISE;
END;
$$;
    return sql;
END;

-- Cleanup old artifacts, accepting that a task may fail once if it happens to run
-- in between the drop and finalize_setup() procedure's completion.
drop procedure if exists internal.get_daily_quota_select();
drop procedure if exists internal.get_recent_quota_usage_select()
