
create or replace function internal.success_to_status(success boolean) returns text
as
$$
    IFF(success IS NULL, NULL, IFF(success, 'SUCCESS', 'FAILURE'))
$$;

create or replace function internal.task_complete(state text) returns boolean
as
$$
    state in ('SUCCEEDED', 'FAILED', 'FAILED_AND_AUTO_SUSPENDED')
$$;

create or replace function internal.task_pending(state text) returns boolean
as
$$
    state in ('SCHEDULED', 'EXECUTING')
$$;

create table if not exists internal.task_create_materialization_status(run timestamp_ltz, success boolean, output variant);
create or replace view reporting.task_create_materialization_status as select * from internal.task_create_materialization_status;

create or replace procedure internal.create_materialization_status()
    returns text
    language sql
    execute as owner
AS
BEGIN
    let s text := $$
    create or replace view admin.materialization_status
    copy grants
    as
    -- The tables we materialize and how often the task refreshes them.
    with task_tables as (
        select table_name, user_schema, user_view, task_name from ( values
            ('QUERY_HISTORY', 'REPORTING', 'ENRICHED_QUERY_HISTORY', 'QUERY_HISTORY_MAINTENANCE'),
            ('WAREHOUSE_EVENTS_HISTORY', 'REPORTING', 'WAREHOUSE_SESSIONS', 'WAREHOUSE_EVENTS_MAINTENANCE'),
            ('WAREHOUSE_EVENTS_HISTORY', 'REPORTING', 'CLUSTER_SESSIONS', 'WAREHOUSE_EVENTS_MAINTENANCE'),
            ('SERVERLESS_TASK_HISTORY', 'REPORTING', 'SERVERLESS_TASK_HISTORY', 'SIMPLE_DATA_EVENTS_MAINTENANCE'),
            ('TASK_HISTORY', 'REPORTING', 'TASK_HISTORY', 'SIMPLE_DATA_EVENTS_MAINTENANCE'),
            ('SESSIONS', 'REPORTING', 'SESSIONS', 'SIMPLE_DATA_EVENTS_MAINTENANCE'),
            ('WAREHOUSE_METERING_HISTORY', 'REPORTING', 'WAREHOUSE_METERING_HISTORY', 'SIMPLE_DATA_EVENTS_MAINTENANCE'),
            ('LOGIN_HISTORY', 'REPORTING', 'LOGIN_HISTORY', 'SIMPLE_DATA_EVENTS_MAINTENANCE'),
            ('HYBRID_TABLE_USAGE_HISTORY', 'REPORTING', 'HYBRID_TABLE_USAGE_HISTORY', 'SIMPLE_DATA_EVENTS_MAINTENANCE'),
            ('MATERIALIZED_VIEW_REFRESH_HISTORY', 'REPORTING', 'MATERIALIZED_VIEW_REFRESH_HISTORY', 'SIMPLE_DATA_EVENTS_MAINTENANCE')
        ) as t(table_name, user_schema, user_view, task_name)
    ), task_history as (
        -- The history rows generated every time a task is run and the table that task is managing
        select $1 as run, $2 as success, $3 as input, $4 as output, $5 as table_name, $6 as task_name, IFF(INPUT is null, 'FULL', 'INCREMENTAL') as kind FROM (
            select run::TIMESTAMP_LTZ, success, input, output, 'QUERY_HISTORY', 'QUERY_HISTORY_MAINTENANCE'   from internal.task_query_history
            union all
            select run::TIMESTAMP_LTZ, success, input, output, 'WAREHOUSE_EVENTS_HISTORY', 'WAREHOUSE_EVENTS_MAINTENANCE' from internal.task_warehouse_events
            union all
            select run::TIMESTAMP_LTZ, success, input, output, table_name, 'SIMPLE_DATA_EVENTS_MAINTENANCE' from internal.task_simple_data_events
        )
    ), sf_task_history as (
        -- Our TASK_HISTORY view is updated with a 3hour delay. Limit how far we back we look in the UDTF.
        select name as task_name, query_id, graph_run_group_id, state, scheduled_time, QUERY_START_TIME,
        from table(information_schema.task_history(SCHEDULED_TIME_RANGE_START => TIMESTAMPADD(HOUR, -3, current_timestamp()), result_limit => 10000))
        WHERE database_name in (select current_database()) and schema_name = 'TASKS'
        union
        select name as task_name, query_id, graph_run_group_id, state, scheduled_time, QUERY_START_TIME,
        from reporting.task_history
        WHERE database_name in (select current_database()) and schema_name = 'TASKS'
    ), completed_tasks as (
        select * from sf_task_history where internal.task_complete(state)
    ), pending_tasks as (
        select * from sf_task_history where internal.task_pending(state)
    ), fullmat as (
        SELECT th.table_name, th.task_name, run, output['end']::TIMESTAMP_LTZ as end, success,
            output['SQLERRM'] as error_message, ct.query_id,
            COALESCE(qht.warehouse_size, qh.warehouse_size) as warehouse_size,
        FROM task_history th
        LEFT JOIN completed_tasks ct ON th.output['task_run_id'] = ct.GRAPH_RUN_GROUP_ID
        -- Get the warehouse_size from QueryHistory. Check both the view and UDTF to avoid gaps.
        LEFT JOIN internal_reporting_mv.query_history_complete_and_daily_incomplete qh ON ct.query_id = qh.query_id
        LEFT JOIN table(information_schema.query_history(END_TIME_RANGE_START => TIMESTAMPADD(HOUR, -3, current_timestamp()), result_limit => 10000)) qht on ct.query_id = qht.query_id
        WHERE (table_name, th.task_name, run) IN (
            SELECT table_name, task_name, MAX(run)
            FROM task_history
            WHERE KIND = 'FULL'
            GROUP BY table_name, task_name
        )
    ), incmat as (
        SELECT table_name, th.task_name, run, output['end']::TIMESTAMP_LTZ as end, success,
            output['SQLERRM'] as error_message, ct.query_id,
            COALESCE(qht.warehouse_size, qh.warehouse_size) as warehouse_size,
        FROM task_history th
        LEFT JOIN completed_tasks ct ON th.output['task_run_id'] = ct.GRAPH_RUN_GROUP_ID
        -- Get the warehouse_size from QueryHistory. Check both the view and UDTF to avoid gaps.
        LEFT JOIN internal_reporting_mv.query_history_complete_and_daily_incomplete qh ON ct.query_id = qh.query_id
        LEFT JOIN table(information_schema.query_history(END_TIME_RANGE_START => TIMESTAMPADD(HOUR, -3, current_timestamp()), result_limit => 1000)) qht on ct.query_id = qht.query_id
        WHERE (table_name, th.task_name, run) IN (
            SELECT table_name, task_name, MAX(run)
            FROM task_history
            WHERE KIND = 'INCREMENTAL'
            GROUP BY table_name, task_name
        )
    )
    select
        -- the user-facing view
        task_tables.user_schema as schema_name,
        task_tables.user_view as table_name,

        -- last full materialization
        fullmat.run as last_full_start,
        fullmat.end as last_full_end,
        internal.success_to_status(fullmat.success) as last_full_status,
        fullmat.error_message::TEXT as last_full_error_message,
        fullmat.query_id as last_full_query_id,
        fullmat.warehouse_size as last_full_warehouse_size,

        -- last incremental materialization
        incmat.run as last_inc_start,
        incmat.end as last_inc_end,
        internal.success_to_status(incmat.success) as last_inc_status,
        incmat.error_message::TEXT as last_inc_error_message,
        incmat.query_id as last_inc_query_id,
        incmat.warehouse_size as last_inc_warehouse_size,

        -- the next invocation
        COALESCE(pt.QUERY_START_TIME, pt.SCHEDULED_TIME) as next_start,
        IFF(fullmat.run IS NULL OR fullmat.success = FALSE, 'FULL', 'INCREMENTAL') as next_type,
        CASE
            WHEN pt.state = 'SCHEDULED' THEN 'PENDING'
            WHEN pt.state = 'EXECUTING' THEN 'RUNNING'
            ELSE NULL
        END as next_status,
        pt.query_id as next_query_id,
    from task_tables
    left join fullmat on task_tables.table_name = fullmat.table_name
    left join incmat on task_tables.table_name = incmat.table_name
    left join pending_tasks pt on task_tables.task_name = pt.task_name
    $$;
    return s;
END;

create view if not exists admin.materialization_status
as
select
    NULL::TEXT as schema_name,
    NULL::TEXT table_name,
    NULL::TIMESTAMP_LTZ as last_full_start,
    NULL::TIMESTAMP_LTZ as last_full_end,
    NULL::TEXT as last_full_status,
    NULL::TEXT as last_full_error_message,
    NULL::TEXT as last_full_query_id,
    NULL::TEXT as last_full_warehouse_size,
    NULL::TIMESTAMP_LTZ as last_inc_start,
    NULL::TIMESTAMP_LTZ as last_inc_end,
    NULL::TEXT as last_inc_status,
    NULL::TEXT as last_inc_error_message,
    NULL::TEXT as last_inc_query_id,
    NULL::TEXT as last_inc_warehouse_size,
    NULL::TIMESTAMP_LTZ as next_start,
    NULL::TEXT as next_type,
    NULL::TEXT as next_status,
    NULL::TEXT as next_query_id
limit 0;
