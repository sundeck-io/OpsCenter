begin
    let dt timestamp := (select dateadd(day, -1, current_timestamp()));
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, object_type, object_name) SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('oldest_running', :dt, 'newest_completed', :dt)::VARIANT, :dt, 'WAREHOUSE_EVENTS', 'WAREHOUSE_EVENTS_HISTORY';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, object_type, object_name) SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('oldest_running', :dt, 'newest_completed', :dt)::VARIANT, :dt, 'QUERY_HISTORY', 'QUERY_HISTORY';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, object_type, object_name) SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, 'SIMPLE_DATA_EVENT', 'SERVERLESS_TASK_HISTORY';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, object_type, object_name) SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, 'SIMPLE_DATA_EVENT', 'TASK_HISTORY';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, object_type, object_name) SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, 'SIMPLE_DATA_EVENT', 'SESSIONS';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, object_type, object_name) SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, 'SIMPLE_DATA_EVENT', 'WAREHOUSE_METERING_HISTORY';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, object_type, object_name) SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, 'SIMPLE_DATA_EVENT', 'LOGIN_HISTORY';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, object_type, object_name) SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, 'SIMPLE_DATA_EVENT', 'HYBRID_TABLE_USAGE_HISTORY';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, object_type, object_name) SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, 'SIMPLE_DATA_EVENT', 'MATERIALIZED_VIEW_REFRESH_HISTORY';

    show warehouses;
    let res resultset := (select "name" as n from table(result_scan(last_query_id())));
    let cur cursor for res;
    for r in cur do
        let wh_name string := r.N;
        insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, object_type, object_name) SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT, :dt, 'WAREHOUSE_LOAD_EVENT', :wh_name;
    end for;
end;
