
create or replace function internal.success_to_status(success boolean) returns text
as
$$
    IFF(success IS NULL, NULL, IFF(success, 'SUCCESS', 'FAILURE'))
$$;

create or replace view admin.materialization_status
as
-- The tables we materialize and how often the task refreshes them.
with task_tables as (
    select table_name, user_schema, user_view from ( values
        ('QUERY_HISTORY', 'REPORTING', 'ENRICHED_QUERY_HISTORY'),
        ('WAREHOUSE_EVENTS_HISTORY', 'REPORTING', 'WAREHOUSE_SESSIONS')
    ) as t(table_name, user_schema, user_view)
), task_history as (
    -- The history rows generated every time a task is run and the table that task is managing
    select $1 as run, $2 as success, $3 as input, $4 as output, $5 as table_name, $6 as task_name, $7 as kind FROM (
        select run, success, input, output, 'QUERY_HISTORY', 'QUERY_HISTORY_MAINTENANCE', IFF(INPUT is null, 'FULL', 'INCREMENTAL')  from internal.task_query_history
        union all
        select run, success, input, output, 'WAREHOUSE_EVENTS_HISTORY', 'WAREHOUSE_EVENTS_MAINTENANCE', IFF(INPUT is null, 'FULL', 'INCREMENTAL') from internal.task_warehouse_events
    )
), sf_task_history as (
    select name as task_name, query_id, graph_run_group_id
    from table(information_schema.task_history(SCHEDULED_TIME_RANGE_START => TIMESTAMPADD(MINUTE, -45, current_timestamp()), result_limit => 1000))
    WHERE database_name in (select current_database()) and schema_name = 'TASKS'
), fullmat as (
    SELECT th.table_name, th.task_name, run, output['end'] as end, success, output['SQLERRM'] as error_message, sf_th.query_id,
    FROM task_history th
    LEFT JOIN sf_task_history sf_th ON th.output['task_run_id'] = sf_th.GRAPH_RUN_GROUP_ID
    WHERE (table_name, th.task_name, run) IN (
        SELECT table_name, task_name, MAX(run)
        FROM task_history
        WHERE KIND = 'FULL'
        GROUP BY table_name, task_name
    )
), incmat as (
    SELECT table_name, th.task_name, run, output['end'] as end, success, output['SQLERRM'] as error_message, sf_th.query_id,
    FROM task_history th
    LEFT JOIN sf_task_history sf_th ON th.output['task_run_id'] = sf_th.GRAPH_RUN_GROUP_ID
    WHERE (table_name, th.task_name, run) IN (
        SELECT table_name, task_name, MAX(run)
        FROM task_history
        WHERE KIND = 'INCREMENTAL'
        GROUP BY table_name, task_name
    )
) select
    task_tables.user_schema as schema_name,
    task_tables.user_view as table_name,
    fullmat.run as last_full_start,
    fullmat.end as last_full_end,
    internal.success_to_status(fullmat.success) as last_full_status,
    fullmat.error_message as last_full_error_message,
    fullmat.query_id as last_full_query_id,
    incmat.run as last_inc_start,
    incmat.end as last_inc_end,
    internal.success_to_status(incmat.success) as last_inc_status,
    incmat.error_message as last_inc_error_message,
    incmat.query_id as last_inc_query_id,
    null::TIMESTAMP_LTZ as next_start,
    null::TEXT as next_type,
    null::TEXT as next_status,
    null::TEXT as next_query_id,
from task_tables
left join fullmat on task_tables.table_name = fullmat.table_name
left join incmat on task_tables.table_name = incmat.table_name;
