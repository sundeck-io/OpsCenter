begin
    let dt timestamp := (select dateadd(day, -1, current_timestamp()));
    let wh_output object := OBJECT_CONSTRUCT('oldest_running', dt, 'newest_completed', dt, 'range_min', :dt, 'range_max', :dt, 'cluster_range_min', :dt, 'cluster_range_max', :dt, 'warehouse_range_min', :dt, 'warehouse_range_max', :dt);
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, task_name, object_name) SELECT :dt, true, null, :wh_output, :dt, 'WAREHOUSE_EVENTS_MAINTENANCE', 'WAREHOUSE_EVENTS_HISTORY';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, task_name, object_name) SELECT :dt, true, null, :wh_output, :dt, 'WAREHOUSE_EVENTS_MAINTENANCE', 'WAREHOUSE_SESSIONS';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, task_name, object_name) SELECT :dt, true, null, :wh_output, :dt, 'WAREHOUSE_EVENTS_MAINTENANCE', 'CLUSTER_SESSIONS';
    let output object := OBJECT_CONSTRUCT('oldest_running', dt, 'newest_completed', dt, 'range_min', :dt, 'range_max', :dt);
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, task_name, object_name) SELECT :dt, true, null, :output, :dt, 'QUERY_HISTORY_MAINTENANCE', 'QUERY_HISTORY';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, task_name, object_name) SELECT :dt, true, null, :output, :dt, 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'SERVERLESS_TASK_HISTORY';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, task_name, object_name) SELECT :dt, true, null, :output, :dt, 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'TASK_HISTORY';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, task_name, object_name) SELECT :dt, true, null, :output, :dt, 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'SESSIONS';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, task_name, object_name) SELECT :dt, true, null, :output, :dt, 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'WAREHOUSE_METERING_HISTORY';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, task_name, object_name) SELECT :dt, true, null, :output, :dt, 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'LOGIN_HISTORY';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, task_name, object_name) SELECT :dt, true, null, :output, :dt, 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'HYBRID_TABLE_USAGE_HISTORY';
    insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, task_name, object_name) SELECT :dt, true, null, :output, :dt, 'SIMPLE_DATA_EVENTS_MAINTENANCE', 'MATERIALIZED_VIEW_REFRESH_HISTORY';

    show warehouses;
    let res resultset := (select "name" as n from table(result_scan(last_query_id())));
    let cur cursor for res;
    for r in cur do
        let wh_name string := r.N;
        insert into "{DATABASE}".INTERNAL.TASK_LOG(task_start, success, input, output, task_finish, task_name, object_name) SELECT :dt, true, null, :output, :dt, 'WAREHOUSE_LOAD_MAINTENANCE', :wh_name;
    end for;
end;
