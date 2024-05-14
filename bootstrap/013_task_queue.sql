
create or replace procedure task_queue.run_tasks()
returns varchar
language sql
as
begin
show warehouses;
show tasks in schema task_queue;
let res resultset := (
with tlist as (
select "id" as id, "name" as name from table(result_scan(last_query_id())) where "name" <> 'run_tasks'
),
wlist as (
select count(*) > 0 as wh_exists from table(result_scan(last_query_id(-2))) where "name" = 'SUNDECK_TASK_QUEUE_WH'
),
thist as (
    select tlist.name, state from tlist left outer join account_usage.task_history th on tlist.id = th.task_id
), tstate as (
    select name, state, row_number() over (partition by name order by state desc) as rn from thist
), alltasks as (
select 'task_queue.' || name as name, state from tstate where rn = 1)
select case when state = 'SUCCEEDED' then 'drop task if exists ' || name || ';'
when wh_exists then 'begin\nalter task '||name||' set warehouse = SUNDECK_TASK_QUEUE_WH;\nexecute task '|| name ||';\nend;'
else 'begin\nalter task '||name||' unset warehouse;\nexecute task '|| name ||';\nend;'
end as cmd from alltasks, wlist where state <> 'RUNNING'
);
let cur cursor for res;
for r in cur do
    let cmd varchar := r.cmd;
    execute immediate cmd;
end for;

end;

create or replace procedure task_queue.create_tasks()
returns varchar
language sql
comment='Create tasks for the task queue, this has to be done after we have permissions in the native app'
as
begin
create task if not exists task_queue.run_tasks_task
schedule = '1440 minute'
allow_overlapping_execution=FALSe
USER_TASK_TIMEOUT_MS = 30000
as
call task_queue.run_tasks();

create task if not exists task_queue.create_warehouse
as
call task_queue.create_warehouse();
end;
create or replace procedure task_queue.create_warehouse()
returns varchar
language sql
as
begin
    create warehouse if not exists sundeck_task_queue_wh warehouse_size = XSMALL warehouse_type = STANDARD auto_suspend = 30 auto_resume = true initially_suspended = true;
    return 'Warehouse created';
end;
