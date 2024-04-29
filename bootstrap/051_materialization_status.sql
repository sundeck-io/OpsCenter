
create or replace function internal.success_to_status(success boolean) returns text
as
$$
    IFF(success IS NULL, NULL, IFF(success, 'SUCCESS', 'FAILURE'))
$$;


create or replace view admin.materialization_status
    copy grants
as
with task_tables as (
    select user_schema, user_view, task_name, object_name, task_period_mins from ( values
        ('REPORTING', 'DBT_HISTORY', 'QUERY_HISTORY_MAINTENANCE', 'QUERY_HISTORY', 60),
        ('REPORTING', 'ENRICHED_QUERY_HISTORY', 'QUERY_HISTORY_MAINTENANCE', 'QUERY_HISTORY', 60),
        ('REPORTING', 'ENRICHED_QUERY_HISTORY_DAILY', 'QUERY_HISTORY_MAINTENANCE', 'QUERY_HISTORY', 60),
        ('REPORTING', 'ENRICHED_QUERY_HISTORY_HOURLY', 'QUERY_HISTORY_MAINTENANCE', 'QUERY_HISTORY', 60),
        ('REPORTING', 'LABELED_QUERY_HISTORY', 'QUERY_HISTORY_MAINTENANCE', 'QUERY_HISTORY', 60),
        ('REPORTING', 'WAREHOUSE_DAILY_UTILIZATION', 'QUERY_HISTORY_MAINTENANCE', 'QUERY_HISTORY', 60),
        ('REPORTING', 'WAREHOUSE_HOURLY_UTILIZATION', 'QUERY_HISTORY_MAINTENANCE', 'QUERY_HISTORY', 60),

        ('REPORTING', 'HYBRID_TABLE_USAGE_HISTORY', 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'HYBRID_TABLE_USAGE_HISTORY', 60),
        ('REPORTING', 'LOGIN_HISTORY', 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'LOGIN_HISTORY', 60),
        ('REPORTING', 'MATERIALIZED_VIEW_REFRESH_HISTORY', 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'MATERIALIZED_VIEW_REFRESH_HISTORY', 60),
        ('REPORTING', 'SERVERLESS_TASK_HISTORY', 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'SERVERLESS_TASK_HISTORY', 60),
        ('REPORTING', 'SESSIONS', 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'SESSIONS', 60),
        ('REPORTING', 'TASK_HISTORY', 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'TASK_HISTORY', 60),
        ('REPORTING', 'WAREHOUSE_METERING_HISTORY', 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'WAREHOUSE_METERING_HISTORY', 60),

        ('REPORTING', 'CLUSTER_SESSIONS', 'WAREHOUSE_EVENTS_MAINTENANCE', 'CLUSTER_SESSIONS', 60),
        ('REPORTING', 'CLUSTER_SESSIONS_DAILY', 'WAREHOUSE_EVENTS_MAINTENANCE', 'CLUSTER_SESSIONS', 60),
        ('REPORTING', 'CLUSTER_SESSIONS_HOURLY', 'WAREHOUSE_EVENTS_MAINTENANCE', 'CLUSTER_SESSIONS', 60),
        ('REPORTING', 'WAREHOUSE_SESSIONS', 'WAREHOUSE_EVENTS_MAINTENANCE', 'WAREHOUSE_SESSIONS', 60),
        ('REPORTING', 'WAREHOUSE_SESSIONS_DAILY', 'WAREHOUSE_EVENTS_MAINTENANCE', 'WAREHOUSE_SESSIONS', 60),
        ('REPORTING', 'WAREHOUSE_SESSIONS_HOURLY', 'WAREHOUSE_EVENTS_MAINTENANCE', 'WAREHOUSE_SESSIONS', 60)
    ) as t(user_schema, user_view, task_name, object_name, task_period_mins)
    UNION ALL
    -- Expand all pairs of WAREHOUSE_LOAD_MAINTENANCE + warehouse_name (as object_name)
    select 'REPORTING', 'WAREHOUSE_LOAD_HISTORY', 'WAREHOUSE_LOAD_MAINTENANCE', object_name, 60 from (select distinct object_name from internal.task_log where task_name = 'WAREHOUSE_LOAD_MAINTENANCE')
), completed_tasks as (
    -- The history rows generated every time a task is run and the table that task is managing
    select task_start, task_finish, success, input, output, task_name, object_name, IFF(INPUT is null, 'FULL', 'INCREMENTAL') as kind, query_id, range_min, range_max,
    FROM INTERNAL.TASK_LOG
    WHERE success is not null
), running_tasks as (
    select task_start, task_finish, success, input, output, task_name, object_name, IFF(INPUT is null, 'FULL', 'INCREMENTAL') as kind, query_id, range_min, range_max,
    FROM INTERNAL.TASK_LOG
    WHERE success is null
), fullmat as (
    SELECT ct.task_name, ct.object_name, ct.task_start, ct.task_finish, ct.success,
        output['SQLERRM'] as error_message, ct.range_min, ct.range_max, ct.query_id,
    FROM completed_tasks ct
    WHERE (ct.object_name, ct.task_name, ct.task_start) IN (
        SELECT object_name, task_name, MAX(task_start)
        FROM completed_tasks
        WHERE KIND = 'FULL'
        GROUP BY object_name, task_name
    )
), incmat as (
    SELECT ct.task_name, ct.object_name, ct.task_start, ct.task_finish, ct.success,
        output['SQLERRM'] as error_message, ct.range_min, ct.range_max, ct.query_id,
    FROM completed_tasks ct
    WHERE (ct.object_name, ct.task_name, ct.task_start) IN (
        SELECT object_name, task_name, MAX(task_start)
        FROM completed_tasks
        WHERE KIND = 'INCREMENTAL'
        GROUP BY object_name, task_name
    )
)
select
    -- the user-facing view
    task_tables.user_schema as schema_name,
    task_tables.user_view as table_name,
    COALESCE(incmat.range_min, fullmat.range_min)::TIMESTAMP_LTZ as range_start,
    COALESCE(incmat.range_max, fullmat.range_min)::TIMESTAMP_LTZ as range_end,
    -- WAREHOUSE_LOAD_MAINTENANCE is unique in that we have multiple discrete objects materialized into a single table
    IFF(task_tables.task_name = 'WAREHOUSE_LOAD_MAINTENANCE', task_tables.object_name, null) as partition,

    -- last full materialization
    fullmat.task_start as last_full_start,
    fullmat.task_finish as last_full_end,
    internal.success_to_status(fullmat.success) as last_full_status,
    fullmat.error_message::TEXT as last_full_error_message,
    fullmat.query_id as last_full_query_id,

    -- last incremental materialization
    incmat.task_start as last_incr_start,
    incmat.task_finish as last_incr_end,
    internal.success_to_status(incmat.success) as last_incr_status,
    incmat.error_message::TEXT as last_incr_error_message,
    incmat.query_id as last_incr_query_id,

    -- the next invocation
    COALESCE(rt.task_start, timestampadd(minutes, task_period_mins, incmat.task_start), timestampadd(minutes, task_period_mins, fullmat.task_start)) as next_start,
    IFF(fullmat.success IS NULL OR fullmat.success = FALSE, 'FULL', 'INCREMENTAL') as next_type,
    IFF(rt.task_start is not null, 'EXECUTING', 'SCHEDULED') as next_status,
    rt.query_id as next_query_id,
from task_tables
left join fullmat on task_tables.task_name = fullmat.task_name AND task_tables.object_name = fullmat.object_name
left join incmat on task_tables.task_name = incmat.task_name AND task_tables.object_name = incmat.object_name
left join running_tasks rt on task_tables.task_name = rt.task_name
ORDER BY schema_name, table_name;
