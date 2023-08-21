
create table if not exists internal.quota_task_history(start_time timestamp_ltz, end_time timestamp_ltz, credits_used object, outcome string);
CREATE OR REPLACE VIEW REPORTING.QUOTA_TASK_HISTORY AS SELECT * FROM INTERNAL.QUOTA_TASK_HISTORY;

-- returns SQL which reads the query_history table function and aggregates the credits used by both user and role. executed within a task
-- as an owner's rights procedure cannot call the query_history table function.
create or replace procedure internal.get_daily_quota_select()
returns string
language sql
as
BEGIN
    let s string := $$
-- TODO this recomputes the usage since midnight every time it runs. We should cache this for the day and update quotas based on deltas.
-- TODO the table func caps out at 10k rows which could miss queries for busy accounts.
with todays_queries as(
    select
        total_elapsed_time,
        credits_used_cloud_services,
        warehouse_size,
        user_name,
        role_name
    from table(information_schema.query_history(RESULT_LIMIT => 10000, END_TIME_RANGE_START => date_trunc('day', current_timestamp()), END_TIME_RANGE_END => current_timestamp()))
    where end_time > date_trunc('day', current_timestamp())
),
costed_queries as (
    select greatest(0,total_elapsed_time)*zeroifnull(wc.credits_per_milli) + credits_used_cloud_services as credits_used, user_name, role_name
    from todays_queries tq left outer join INTERNAL_REPORTING.WAREHOUSE_CREDITS_PER_SIZE wc on tq.warehouse_size = wc.warehouse_size
),
-- Is there a way to collapse user_usage and role_usage together? They're essentially the same query.
user_usage as (
    select user_name as name, sum(credits_used) as usage_map
    from costed_queries
    group by user_name
),
role_usage as (
    select role_name as name, sum(credits_used) as usage_map
    from costed_queries
    group by role_name
),
usage_aggr as (
    select 'users' as kind, object_agg(name, usage_map) as credits_used from user_usage union all select 'roles', object_agg(name, usage_map) from role_usage
)
-- Aggregate into a final map {'users': {..}, 'roles': {..}}
select object_agg(kind, credits_used) as daily_quota from usage_aggr;
$$;
    return s;
END;

create or replace function internal.is_consumption_enabled()
returns boolean
as
$$
    select coalesce((select TRY_TO_BOOLEAN(value) from internal.config where key = 'generate_consumption'), TRUE)
$$;

-- Create the 'generate_consumption' config value set to TRUE only when it doesn't exist
MERGE INTO internal.config AS target
    USING (SELECT 'generate_consumption' AS key, 'TRUE' AS value
    ) AS source
    ON target.key = source.key
    WHEN NOT MATCHED THEN
        INSERT (key, value)
            VALUES (source.key, source.value);
