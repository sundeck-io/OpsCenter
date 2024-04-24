
create or replace function admin.materialization_status()
    returns table(table_name text, full_materialization_complete boolean, range_min timestamp_ltz, range_max timestamp_ltz, last_execution object, current_execution object, next_execution object)
    language sql
as
$$
-- The tables we materialize and how often the task refreshes them.
with task_tables as (
    select table_name, task_period_minutes from ( values
        ('QUERY_HISTORY', 60),
        ('WAREHOUSE_EVENTS_HISTORY', 60)
    ) as t(table_name, task_period_minutes)
), task_history as (
    -- The history rows generated every time a task is run and the table that task is managing
    select run, success, input, output, 'QUERY_HISTORY' as table_name from internal.task_query_history
    union all
    select run, success, input, output, 'WAREHOUSE_EVENTS_HISTORY' as table_name from internal.task_warehouse_events
), default_runs as (
    -- Make some default rows in case we have no materializations recorded
    select table_name, current_timestamp() as run, null as success, null as kind, null as error_message from (select table_name from task_tables)
), last_runs as (
    -- Get the last task execution for each table.
    SELECT table_name, run, success, IFF(INPUT IS NULL, 'FULL', 'INCREMENTAL') as kind, output['SQLERRM'] as error_message FROM task_history
    WHERE (table_name, run) IN (
        SELECT table_name, MAX(run)
        FROM task_history
        GROUP BY table_name
    )
), last_runs_with_defaults as (
    select dr.table_name, coalesce(lr.run, dr.run) as run, coalesce(lr.success, dr.success) as success, coalesce(lr.kind, dr.kind) as kind, coalesce(lr.error_message, dr.error_message) as error_message
    from default_runs dr
    left join last_runs lr on lr.table_name = dr.table_name
), full_materializations as (
    -- At least one successful materialization implies the full materialization has been done.
    select table_name, count(*) > 0 full_materialization_complete from task_history where success group by table_name
), available_data as (
    select $1 as table_name, $2 as range_min, $3 as range_max from (
        select 'QUERY_HISTORY', MIN(start_time), MAX(start_time) from reporting.enriched_query_history
        UNION ALL
        select 'WAREHOUSE_EVENTS_HISTORY', MIN(session_start), MAX(session_start) from reporting.warehouse_sessions
    )
) select task_tables.table_name,
        COALESCE(full_materialization_complete, FALSE) as full_materialization_complete,
        available_data.range_min,
        available_data.range_max,
        IFF(last_runs_with_defaults.kind is null, null, {'start': run, 'success': success, 'kind': last_runs_with_defaults.kind, 'error_message': error_message}) as last_execution,
        NULL as current_execution,
        {'estimated_start': timestampadd(MINUTE, task_period_minutes, run),
            'kind': COALESCE(IFF(success, 'INCREMENTAL', last_runs_with_defaults.kind), 'FULL')
        } as next_execution,
    from last_runs_with_defaults
    left join task_tables on last_runs_with_defaults.table_name = task_tables.table_name
    left join full_materializations on last_runs_with_defaults.table_name = full_materializations.table_name
    left join available_data on last_runs_with_defaults.table_name = available_data.table_name
$$;
