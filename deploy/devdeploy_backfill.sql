begin
    let dt timestamp := (select dateadd(day, -1, current_timestamp()));
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, task_name, object_name) SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('oldest_running', :dt, 'newest_completed', :dt)::VARIANT, :dt, 'WAREHOUSE_EVENTS_MAINTENANCE', 'WAREHOUSE_EVENTS_HISTORY';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, task_name, object_name) SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('oldest_running', :dt, 'newest_completed', :dt)::VARIANT, :dt, 'QUERY_HISTORY_MAINTENANCE', 'QUERY_HISTORY';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, task_name, object_name) SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'SERVERLESS_TASK_HISTORY';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, task_name, object_name) SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'TASK_HISTORY';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, task_name, object_name) SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'SESSIONS';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, task_name, object_name) SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'WAREHOUSE_METERING_HISTORY';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, task_name, object_name) SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'LOGIN_HISTORY';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, task_name, object_name) SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'HYBRID_TABLE_USAGE_HISTORY';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, task_name, object_name) SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'MATERIALIZED_VIEW_REFRESH_HISTORY';

    show warehouses;
    let res resultset := (select "name" as n from table(result_scan(last_query_id())));
    let cur cursor for res;
    for r in cur do
        let wh_name string := r.N;
        insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, task_name, object_name) SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, 'WAREHOUSE_LOAD_MAINTENANCE', :wh_name;
    end for;
end;
