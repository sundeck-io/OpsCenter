
create or replace procedure task_queue.run_tasks(database varchar)
returns object
language sql
as
begin
call task_queue.create_warehouse();
show warehouses;
show tasks in schema task_queue;
let cmd varchar := (
with tlist as (
select "id" as id, "name" as name from table(result_scan(last_query_id())) where "name" <> 'run_tasks'
),
wlist as (
select count(*) > 0 as wh_exists from table(result_scan(last_query_id(-2))) where "name" = 'SUNDECK_TASK_QUEUE_WH'
),
thist as (
    select tlist.name, state, completed_time from tlist left outer join account_usage.task_history th on tlist.id = th.task_id where th.database_name = :database and schema_name='TASK_QUEUE'
), tstate as (
    select name, state, row_number() over (partition by name order by completed_time desc) as rn from thist
), alltasks as (
select 'task_queue.' || name as name, state from tstate where rn = 1), cmdlist as (
select case when state = 'SUCCEEDED' then 'drop task if exists ' || name || ';'
when wh_exists then 'begin\nalter task '||name||' set warehouse = SUNDECK_TASK_QUEUE_WH;\nexecute task '|| name ||';\nend;'
else 'begin\nalter task '||name||' unset warehouse;\nexecute task '|| name ||';\nend;'
end as cmd from alltasks, wlist where state <> 'RUNNING'
)
select 'BEGIN\n' || array_to_string(array_agg(cmd),';\n') || 'END;' as cmd from cmdlist
);
);
execute immediate cmd;
return OBJECT_CONSTRUCT('cmd', :cmd, 'status', 'success'};
exception
when other then
    return OBJECT_CONSTRUCT('Error type', 'Other error', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate, 'status', 'failed');
end;

create or replace procedure task_queue.create_tasks()
returns varchar
language sql
comment='Create tasks for the task queue, this has to be done after we have permissions in the native app'
as
begin
create task if not exists task_queue.run_tasks_task
schedule = '1440 minute'
allow_overlapping_execution=FALSE
USER_TASK_TIMEOUT_MS = 30000
as
DECLARE
    task_name text default 'TASK_QUEUE_MAINTENANCE';
    object_name text default 'TASK_QUEUE';
    root_task_id text default (select INTERNAL.ROOT_TASK_ID());
    task_run_id text default (select INTERNAL.TASK_RUN_ID());
begin
    let input object;
    CALL INTERNAL.START_TASK(:task_name, :object_name, :task_run_id, :query_id) into :input;

    let output object;
    call task_queue.run_tasks(current_database()) into :output;

    CALL INTERNAL.FINISH_TASK(:task_name, :object_name, :task_run_id, :output);

end;

create or replace procedure task_queue.create_warehouse()
returns boolean
language sql
as
begin
    create warehouse if not exists sundeck_task_queue_wh warehouse_size = XSMALL warehouse_type = STANDARD auto_suspend = 30 auto_resume = true initially_suspended = true;
    return true;
exception
when other then
    return false;
end;
