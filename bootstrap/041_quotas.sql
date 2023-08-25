
create table if not exists internal.quota_task_history(start_time timestamp_tz, end_time timestamp_tz, credits_used object, outcome string);
CREATE OR REPLACE VIEW REPORTING.QUOTA_TASK_HISTORY AS SELECT * FROM INTERNAL.QUOTA_TASK_HISTORY;

create table if not exists internal.aggregated_hourly_quota(day date, hour_of_day integer, name string, persona string, credits_used float, last_updated timestamp_tz);

-- We want to read the last 1 hour plus N minutes where N is the number of minutes since top of the hour.
-- This has the limitation that if there are more than 10K queries run in a two-hour window, we will miss some.
CREATE OR REPLACE FUNCTION INTERNAL.GET_QUERY_HISTORY_FUNC_START_RANGE(t TIMESTAMP_TZ)
RETURNS TIMESTAMP_TZ
AS
$$
    timestampadd('hour', -1, date_trunc('hour', t))
$$;

-- Cleanup old artifacts, accepting that a task may fail once if it happens to run
-- in between the drop and finalize_setup() procedure's completion.
drop procedure if exists internal.get_daily_quota_select();
drop procedure if exists internal.get_recent_quota_usage_select();
drop PROCEDURE if exists INTERNAL.REFRESH_HOURLY_QUERY_USAGE_SQL(TIMESTAMP);
