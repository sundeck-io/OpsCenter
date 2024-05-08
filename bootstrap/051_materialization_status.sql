
create or replace view admin.materialization_status
    copy grants
as
WITH RANKED_RAW AS (
    select
        task_start,
        task_finish,
        case
            when success is null then null
            when success then 'SUCCESS'
            else 'FAILURE'
        end as success,
        input,
        output,
        task_name,
        object_name,
        INPUT is null as full_refresh,
        SUCCESS IS NULL AS RUNNING,
        output['SQLERRM']::TEXT as error_message,
        query_id,
        range_min,
        range_max,
    FROM ADMIN.TASK_LOG_HISTORY
    -- limit to the last record of each type.
    QUALIFY ROW_NUMBER() OVER (PARTITION BY task_name, object_name, full_refresh, running ORDER BY task_start DESC) = 1
),

summary as (
select
    task_name,
    object_name,
    -- Take the range min/max from the last incr if it exists, else from the last full.
    COALESCE(MAX(IFF(NOT full_refresh, range_min, null)),
        MAX(IFF(full_refresh, range_min, null))) as range_start,
    COALESCE(MAX(IFF(NOT full_refresh, range_max, null)),
        MAX(IFF(full_refresh, range_max, null))) as range_end,

    -- ANY_VALUE will pick any row within the window and the evaluating the IFF.
    -- MAX, however, will evaluate the condition first, and then select a value.
    MAX(IFF(full_refresh AND NOT RUNNING, task_start, null)) AS last_full_start,
    MAX(IFF(full_refresh AND NOT RUNNING, task_finish, null)) AS last_full_end,
    MAX(IFF(full_refresh AND NOT RUNNING, success, null)) AS last_full_status,
    MAX(IFF(full_refresh AND NOT RUNNING, error_message, null)) AS last_full_error_message,
    MAX(IFF(full_refresh AND NOT RUNNING, query_id, null)) AS last_full_query_id,

    MAX(IFF(NOT full_refresh AND NOT RUNNING, task_start, null)) AS last_incr_start,
    MAX(IFF(NOT full_refresh AND NOT RUNNING, task_finish, null)) AS last_incr_end,
    MAX(IFF(NOT full_refresh AND NOT RUNNING, success, null)) AS last_incr_status,
    MAX(IFF(NOT full_refresh AND NOT RUNNING, error_message, null)) AS last_incr_error_message,
    MAX(IFF(NOT full_refresh AND NOT RUNNING, query_id, null)) AS last_incr_query_id,

    MAX(IFF(NOT RUNNING, task_start, null)) AS last_start,

    MAX(IFF(RUNNING, task_start, null)) AS running_start,
    MAX(IFF(RUNNING, task_finish, null)) AS running_end,
    MAX(IFF(RUNNING, query_id, null)) AS running_query_id,
    MAX(IFF(RUNNING, IFF(full_refresh, 'FULL', 'INCREMENTAL'), null)) AS running_type,
    FROM RANKED_RAW
    GROUP BY task_name, object_name
),

-- don't report the last incremental unless it comes after a successful full refresh.
summary_onlyincrpostfull as (
select
    last_incr_start > last_full_end and last_full_status = 'SUCCESS' as incr_valid,
    * REPLACE (
    IFF(incr_valid, last_incr_start, null) as last_incr_start,
    IFF(incr_valid, last_incr_end, null) as last_incr_end,
    IFF(incr_valid, last_incr_status, null) as last_incr_status,
    IFF(incr_valid, last_incr_error_message, null) as last_incr_error_message,
    IFF(incr_valid, last_incr_query_id, null) as last_incr_query_id
    )
    FROM summary
),

task_tables as (
    select * from ( values
        ('REPORTING', 'DBT_HISTORY', 'QUERY_HISTORY_MAINTENANCE', 'QUERY_HISTORY', 60),
        ('REPORTING', 'ENRICHED_QUERY_HISTORY', 'QUERY_HISTORY_MAINTENANCE', 'QUERY_HISTORY', 60),
        ('REPORTING', 'ENRICHED_QUERY_HISTORY_DAILY', 'QUERY_HISTORY_MAINTENANCE', 'QUERY_HISTORY', 60),
        ('REPORTING', 'ENRICHED_QUERY_HISTORY_HOURLY', 'QUERY_HISTORY_MAINTENANCE', 'QUERY_HISTORY', 60),
        ('REPORTING', 'LABELED_QUERY_HISTORY', 'QUERY_HISTORY_MAINTENANCE', 'QUERY_HISTORY', 60),
        ('REPORTING', 'WAREHOUSE_DAILY_UTILIZATION', 'QUERY_HISTORY_MAINTENANCE', 'QUERY_HISTORY', 60),
        ('REPORTING', 'WAREHOUSE_HOURLY_UTILIZATION', 'QUERY_HISTORY_MAINTENANCE', 'QUERY_HISTORY', 60),

        ('REPORTING', 'HYBRID_TABLE_USAGE_HISTORY', 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'HYBRID_TABLE_USAGE_HISTORY', 720),
        ('REPORTING', 'LOGIN_HISTORY', 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'LOGIN_HISTORY', 720),
        ('REPORTING', 'MATERIALIZED_VIEW_REFRESH_HISTORY', 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'MATERIALIZED_VIEW_REFRESH_HISTORY', 720),
        ('REPORTING', 'SERVERLESS_TASK_HISTORY', 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'SERVERLESS_TASK_HISTORY', 720),
        ('REPORTING', 'SESSIONS', 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'SESSIONS', 720),
        ('REPORTING', 'TASK_HISTORY', 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'TASK_HISTORY', 720),
        ('REPORTING', 'WAREHOUSE_METERING_HISTORY', 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'WAREHOUSE_METERING_HISTORY', 720),

        ('REPORTING', 'CLUSTER_SESSIONS', 'WAREHOUSE_EVENTS_MAINTENANCE', 'CLUSTER_SESSIONS', 60),
        ('REPORTING', 'CLUSTER_SESSIONS_DAILY', 'WAREHOUSE_EVENTS_MAINTENANCE', 'CLUSTER_SESSIONS', 60),
        ('REPORTING', 'CLUSTER_SESSIONS_HOURLY', 'WAREHOUSE_EVENTS_MAINTENANCE', 'CLUSTER_SESSIONS', 60),
        ('REPORTING', 'WAREHOUSE_SESSIONS', 'WAREHOUSE_EVENTS_MAINTENANCE', 'WAREHOUSE_SESSIONS', 60),
        ('REPORTING', 'WAREHOUSE_SESSIONS_DAILY', 'WAREHOUSE_EVENTS_MAINTENANCE', 'WAREHOUSE_SESSIONS', 60),
        ('REPORTING', 'WAREHOUSE_SESSIONS_HOURLY', 'WAREHOUSE_EVENTS_MAINTENANCE', 'WAREHOUSE_SESSIONS', 60)
    ) as t(user_schema, user_view, task_name, object_name, task_period_mins)
),

expanded as (
    select
        -- WAREHOUSE_LOAD_MAINTENANCE won't join below so we have to statically populate the values from that here.
        IFF(s.task_name = 'WAREHOUSE_LOAD_MAINTENANCE', 'REPORTING', user_schema) as user_schema,
        IFF(s.task_name = 'WAREHOUSE_LOAD_MAINTENANCE', 'WAREHOUSE_LOAD_HISTORY', user_view) as user_view,
        IFF(s.task_name = 'WAREHOUSE_LOAD_MAINTENANCE', s.object_name, null) as partition,
        range_start,
        range_end,
        last_full_start,
        last_full_end,
        last_full_status,
        last_full_error_message,
        last_full_query_id,
        last_incr_start,
        last_incr_end,
        last_incr_status,
        last_incr_error_message,
        last_incr_query_id,
        -- 60*7=420mins which is the schedule for WAREHOUSE_LOAD_MAINTENANCE
        COALESCE(running_start, timestampadd(minutes, coalesce(task_period_mins, 420), last_start)) as next_start,
        COALESCE(running_type, IFF(last_full_status <> 'SUCCESS', 'FULL', 'INCREMENTAL')) as next_type,
        IFF(running_type is not null, 'EXECUTING', 'SCHEDULED') as next_status,
        running_query_id as next_query_id,
    from summary_onlyincrpostfull s
    LEFT JOIN task_tables t on s.task_name = t.task_name and s.object_name = t.object_name

    -- exclude non-matching rows other than warehouse load maintenance.
    WHERE t.task_name is not null OR s.task_name = 'WAREHOUSE_LOAD_MAINTENANCE'
)

select * from expanded;
