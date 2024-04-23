
create or replace view internal.all_task_history(run, success, input, output, table_name) as
    select run, success, input, output, 'QUERY_HISTORY' from internal.task_query_history
    union all
    select run, success, input, output, 'WAREHOUSE_EVENTS_HISTORY'  from internal.task_warehouse_events;


create or replace function admin.materialization_status()
    returns table(table_name text, full_materialization_complete boolean, last_execution object, current_execution object, next_execution object)
as
$$
-- The tables we materialize and how often the task refreshes them.
with tables as (
    select table_name, task_period_minutes from ( values
        ('QUERY_HISTORY', 60),
        ('WAREHOUSE_EVENTS_HISTORY', 60)
    ) as t(table_name, task_period_minutes)
), last_runs as (
    -- Get the last task execution for each table.
    SELECT table_name, run, success, IFF(INPUT IS NULL, 'FULL', 'INCREMENTAL') as kind, output['SQLERRM'] as error_message FROM internal.all_task_history
    WHERE (table_name, run) IN (
        SELECT table_name, MAX(run)
        FROM internal.all_task_history
        GROUP BY table_name
    )
), full_materializations as (
    -- Check if we have completed at least one full materialization
    select table_name, count(*) > 0 full_materialization_complete from internal.all_task_history where success and IFF(input is null, FALSE, input['newest_completed'] is not null) group by table_name
) select tables.table_name,
        full_materialization_complete,
        {'start': run, 'success': success, 'kind': last_runs.kind} as last_execution,
        NULL as current_execution,
        {'estimated_start': timestampadd(MINUTE, task_period_minutes, run), 'kind': IFF(last_runs.kind is null, 'FULL', 'INCREMENTAL')} as next_execution,
    from last_runs
    left join tables on tables.table_name = last_runs.table_name
    left join full_materializations on tables.table_name = full_materializations.table_name
$$;
