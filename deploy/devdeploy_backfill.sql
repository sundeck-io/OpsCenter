begin
    let dt timestamp := (select dateadd(day, -1, current_timestamp()));
		-- Leave a note in the OUTPUT to indicate these rows are coming from the devdeploy setup (not within the app)
		insert into "{DATABASE}".INTERNAL.TASK_WAREHOUSE_EVENTS SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('devdeploy', 'true', 'oldest_running', :dt, 'newest_completed', :dt, 'end', current_timestamp())::VARIANT;
    insert into "{DATABASE}".INTERNAL.TASK_QUERY_HISTORY SELECT current_timestamp(), true, null, OBJECT_CONSTRUCT('devdeploy', 'true', 'oldest_running', :dt, 'newest_completed', :dt, 'end', current_timestamp())::VARIANT;

    insert into "{DATABASE}".INTERNAL.TASK_SIMPLE_DATA_EVENTS SELECT current_timestamp(), true, 'SERVERLESS_TASK_HISTORY', null, OBJECT_CONSTRUCT('devdeploy', 'true', 'oldest_running', :dt, 'end', current_timestamp())::VARIANT;
    insert into "{DATABASE}".INTERNAL.TASK_SIMPLE_DATA_EVENTS SELECT current_timestamp(), true, 'TASK_HISTORY', null, OBJECT_CONSTRUCT('devdeploy', 'true', 'oldest_running', :dt, 'end', current_timestamp())::VARIANT;
    insert into "{DATABASE}".INTERNAL.TASK_SIMPLE_DATA_EVENTS SELECT current_timestamp(), true, 'SESSIONS', null, OBJECT_CONSTRUCT('devdeploy', 'true', 'oldest_running', :dt, 'end', current_timestamp())::VARIANT;
    insert into "{DATABASE}".INTERNAL.TASK_SIMPLE_DATA_EVENTS SELECT current_timestamp(), true, 'WAREHOUSE_METERING_HISTORY', null, OBJECT_CONSTRUCT('devdeploy', 'true', 'oldest_running', :dt, 'end', current_timestamp())::VARIANT;

    show warehouses;
    let res resultset := (select "name" as n from table(result_scan(last_query_id())));
    let cur cursor for res;
    for r in cur do
        let wh_name string := r.N;
        insert into "{DATABASE}".INTERNAL.TASK_WAREHOUSE_LOAD_EVENTS SELECT current_timestamp(), true, :wh_name, null, OBJECT_CONSTRUCT('oldest_running', :dt)::VARIANT;
    end for;
end;
