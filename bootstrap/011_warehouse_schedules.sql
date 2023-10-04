
-- NB. Table is created in 090_post_setup.sql because you cannot call python procs from the setup.sql
CREATE OR REPLACE PROCEDURE INTERNAL.CREATE_WAREHOUSE_SCHEDULES_VIEWS()
    RETURNS BOOLEAN
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
BEGIN
    CREATE OR REPLACE VIEW catalog.warehouse_schedules COPY GRANTS AS SELECT * exclude (id_val, day) FROM internal.wh_schedules;
    CREATE OR REPLACE VIEW reporting.warehouse_schedules_task_history as
        SELECT run, success, output:"statements"::ARRAY as statements_executed, output:"opscenter timezone"::TEXT as schedule_timezone, output:"warehouses_updated"::NUMBER as warehouses_updated
        from internal.task_warehouse_schedule;
    -- Because we defer creation of the table until FINALIZE_SETUP, we need to re-run
    -- the grant commands to ensure that the user can see this view because it would
    -- not receive the grant during 100_final_perms.
    GRANT SELECT ON ALL VIEWS IN SCHEMA CATALOG TO APPLICATION ROLE ADMIN;
    GRANT SELECT ON ALL VIEWS IN SCHEMA CATALOG TO APPLICATION ROLE READ_ONLY;
    GRANT SELECT ON ALL VIEWS IN SCHEMA REPORTING TO APPLICATION ROLE ADMIN;
    GRANT SELECT ON ALL VIEWS IN SCHEMA REPORTING TO APPLICATION ROLE READ_ONLY;
    return TRUE;
END;

CREATE OR REPLACE PROCEDURE INTERNAL.UPDATE_WAREHOUSE_SCHEDULES(last_run timestamp_ltz, this_run timestamp_ltz)
    RETURNS VARIANT
    language sql
 AS
DECLARE
    task_outcome variant default (select object_construct());
BEGIN
    -- Get the configured timezone or default to 'America/Los_Angeles'
    let tz text;
    call internal.get_config('default_timezone') into :tz;
    if (tz is null) then
        tz := 'America/Los_Angeles';
    end if;
    task_outcome := (select object_insert(:task_outcome, 'opscenter timezone', :tz));
    task_outcome := (select object_insert(:task_outcome, 'account timezone', internal.get_current_timezone()));

    -- The task calls this procedure with NULL and lets the procedure figure out the details.
    -- The ability to specify timestamps is only to enable testing.
    if (this_run is NULL) then
        this_run := (select CONVERT_TIMEZONE(internal.get_current_timezone(), :tz, current_timestamp()));
    else
        this_run := (select CONVERT_TIMEZONE(internal.get_current_timezone(), :tz, :this_run));
    end if;
    task_outcome := (select object_insert(:task_outcome, 'this_run', :this_run));

    if (last_run is NULL) then
        last_run := (select CONVERT_TIMEZONE(internal.get_current_timezone(), :tz, run) from internal.task_warehouse_schedule order by run desc limit 1);
        -- If we don't have any rows in internal.task_warehouse_schedule, rewind far enough that we will just pick
        -- the current WH schedule and not think it has already been run.
        if (last_run is NULL) then
            last_run := (select CONVERT_TIMEZONE(internal.get_current_timezone(), :tz, timestampadd('days', -1, current_timestamp)));
        end if;
    else
        last_run := (select CONVERT_TIMEZONE(internal.get_current_timezone(), :tz, :last_run));
    end if;

    -- TODO handle looking back over a weekend boundary (from python)
    -- TODO the WEEK_START session parameter can alter what DAYOFWEEK returns.
    let is_weekday boolean := (select DAYOFWEEK(:this_run) not in (0, 6));

    -- Get the warehouse schedule to apply.
    -- Get the latest (greatest start_at), enabled schedules which match the day of the week that should be applied
    -- since the last time the task ran.
    let res resultset := (
        with schedules as(
          select *, TIMESTAMP_FROM_PARTS(date_trunc('day', :this_run)::DATE, start_at) as ts
          from internal.wh_schedules
          where
              enabled = TRUE and
              weekday = :is_weekday and
              ts > :last_run and ts <= :this_run
        ), ids as(
            select distinct(last_value(id_val)) over (partition by name order by ts) from schedules
        ) select sch.name, sch.id_val, alt.alter_statement
            from internal.wh_schedules sch
                left outer join internal.warehouse_alter_statements alt on sch.id_val = alt.id_val
            where sch.id_val in (select * from ids)
    );

    -- build some metadata
    let ids array := (select array_agg("ID_VAL") from table(result_scan(last_query_id())));
    task_outcome := (select object_insert(:task_outcome, 'num_candidates', array_size(:ids)));
    task_outcome := (select object_insert(:task_outcome, 'candidates', :ids));
    task_outcome := (select object_insert(:task_outcome, 'last_run', :last_run));

    let c1 cursor for res;

    let success boolean := TRUE;
    let warehouses_updated := 0;
    let statements array := array_construct();
    for schedule in c1 do
        let sch_outcome string := '';
        begin
            -- Make sure the about left outer join matched a row.
            if (length(coalesce(schedule.alter_statement, '')) = 0) then
                sch_outcome := 'No alter statement found for warehouse ' || schedule.name;
                success := FALSE;
            else
                -- Return the actual alter statement to know what was executed.
                sch_outcome := schedule.alter_statement;
                execute immediate schedule.alter_statement;
                warehouses_updated := warehouses_updated + 1;
            end if;
        exception
            when other then
                sch_outcome := 'Exception occurred when applying warehouse schedules for ' || schedule.name || ', error: ' || :sqlerrm;
                SYSTEM$LOG_ERROR(OBJECT_CONSTRUCT('error', :sch_outcome,
                    'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate));
                success := FALSE;
        end;
        statements := (select array_append(:statements, :sch_outcome));
    end for;
    task_outcome := (select object_insert(:task_outcome, 'warehouses_updated', :warehouses_updated));
    task_outcome := (select object_insert(:task_outcome, 'statements', :statements));

    INSERT INTO internal.task_warehouse_schedule SELECT :this_run, :success, :task_outcome;
    return task_outcome;
EXCEPTION
    WHEN OTHER THEN
        SYSTEM$LOG_ERROR(OBJECT_CONSTRUCT('error', 'Exception occurred when applying warehouse schedules.', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate));
        task_outcome := (select object_insert(object_insert(:task_outcome, 'SQLERRM', :sqlerrm), 'SQLSTATE', :sqlstate));
        INSERT INTO internal.task_warehouse_schedule SELECT :this_run, FALSE, :task_outcome;
        RAISE;
END;

-- owners rights procedures can't get the timezone from the session parameters.
CREATE OR REPLACE FUNCTION INTERNAL.GET_CURRENT_TIMEZONE()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    return Intl.DateTimeFormat().resolvedOptions().timeZone;
$$;


CREATE OR REPLACE PROCEDURE ADMIN.CREATE_WAREHOUSE_SCHEDULE(warehouse_name text, size text, start_at time, finish_at time, is_weekday boolean, suspend_minutes number, autoscale_mode text, autoscale_min number, autoscale_max number, auto_resume boolean, comment text)
    RETURNS TEXT
    LANGUAGE PYTHON
    runtime_version = "3.10"
    handler = 'create_warehouse_schedule'
    packages = ('snowflake-snowpark-python', 'pydantic')
    imports = ('{{stage}}/python/crud.zip')
    EXECUTE AS OWNER
AS
$$
import datetime
import uuid
from crud.wh_sched import WarehouseSchedules, after_schedule_change, merge_new_schedule, verify_and_clean
def create_warehouse_schedule(session, name: str, size: str, start: datetime.time, finish: datetime.time, weekday: bool, suspend_minutes: int, autoscale_mode: str, autoscale_min: int, autoscale_max: int, auto_resume: bool, comment: str):
    current_scheds = WarehouseSchedules.find_all(session, name, weekday)

    # Figure out if the schedules are enabled or disabled
    is_enabled = all(s.enabled for s in current_scheds) if len(current_scheds) > 0 else False

    new_sched = WarehouseSchedules.parse_obj(dict(
        id_val=uuid.uuid4().hex,
        name=name,
        size=size,
        start_at=start,
        finish_at=finish,
        suspend_minutes=suspend_minutes,
        warehouse_mode=autoscale_mode,
        scale_min=autoscale_min,
        scale_max=autoscale_max,
        resume=auto_resume,
        weekday=weekday,
        enabled=is_enabled,
        comment=comment,
    ))

    # Handles pre-existing schedules or no schedules for this warehouse.
    new_scheds = merge_new_schedule(new_sched, current_scheds)

    err_msg, new_scheds = verify_and_clean(new_scheds)
    if err_msg is not None:
        raise Exception(f"Failed to create schedule for {name}, {err_msg}")

    # Write the new schedule
    new_sched.write(session)
    # Update any schedules that were affected by adding the new schedule
    [i.update(session, i) for i in new_scheds if i.id_val != new_sched.id_val]
    # Twiddle the task state after adding a new schedule
    after_schedule_change(session)
$$;


CREATE OR REPLACE PROCEDURE ADMIN.DELETE_WAREHOUSE_SCHEDULE(name text, start_at time, finish_at time, weekday boolean)
    RETURNS TEXT
    LANGUAGE PYTHON
    runtime_version = "3.10"
    handler = 'run_delete'
    packages = ('snowflake-snowpark-python', 'pydantic')
    imports = ('{{stage}}/python/crud.zip')
    EXECUTE AS OWNER
AS
$$
import datetime
from crud.wh_sched import WarehouseSchedules, after_schedule_change, delete_warehouse_schedule
def run_delete(session, name: str, start: datetime.time, finish: datetime.time, is_weekday: bool):
    # Find the matching schedule
    row = WarehouseSchedules.find_one(session, name, start, finish, is_weekday)
    if not row:
        raise Exception(f"Could not find warehouse schedule: {name}, {start}, {finish}, {'weekday' if is_weekday else 'weekend'}")

    to_delete = WarehouseSchedules.construct(id_val = row.id_val)
    current_scheds = WarehouseSchedules.batch_read(session, filter=lambda df: ((df.name == name) & (df.weekday == is_weekday)))
    new_scheds = delete_warehouse_schedule(to_delete, current_scheds)

    # Delete that schedule, leaving a hole
    to_delete.delete(session)

    # Run the updates, filling the hole
    [i.update(session, i) for i in new_scheds]

    # Twiddle the task state after adding a new schedule
    after_schedule_change(session)
$$;


CREATE OR REPLACE PROCEDURE ADMIN.UPDATE_WAREHOUSE_SCHEDULE(warehouse_name text, start_at time, finish_at time, weekday boolean, size text, suspend_minutes number, autoscale_mode text, autoscale_min number, autoscale_max number, auto_resume boolean, comment text)
    RETURNS TEXT
    LANGUAGE PYTHON
    runtime_version = "3.10"
    handler = 'update_warehouse_schedule'
    packages = ('snowflake-snowpark-python', 'pydantic')
    imports = ('{{stage}}/python/crud.zip')
    EXECUTE AS OWNER
AS
$$
import datetime
from crud.wh_sched import WarehouseSchedules, after_schedule_change, update_existing_schedule
def update_warehouse_schedule(session, name: str, start: datetime.time, finish: datetime.time, is_weekday: bool, size: str, suspend_minutes: int, autoscale_mode: str, autoscale_min: int, autoscale_max: int, auto_resume: bool, comment: str):
    # Find a matching schedule
    old_schedule = WarehouseSchedules.find_one(session, name, start, finish, is_weekday)
    if not old_schedule:
        raise Exception(f"Could not find warehouse schedule: {name}, {start}, {finish}, {'weekday' if is_weekday else 'weekend'}")

    # Make the new version of that schedule with the same id_val
    new_schedule = WarehouseSchedules.parse_obj(dict(
        id_val=old_schedule.id_val,
        name=name,
        size=size,
        start_at=start,
        finish_at=finish,
        suspend_minutes=suspend_minutes,
        warehouse_mode=autoscale_mode,
        scale_min=autoscale_min,
        scale_max=autoscale_max,
        resume=auto_resume,
        comment=comment,
        enabled=old_schedule.enabled,
    ))

    # Read the current schedules
    schedules = WarehouseSchedules.find_all(session, name, new_schedule.weekday)

    # Update the WarehouseSchedule instance for this warehouse
    schedules_needing_update = update_existing_schedule(old_schedule.id_val, new_schedule, schedules)

    # Persist all updates to the table
    [i.update(session, i) for i in schedules_needing_update]

    # Twiddle the task state after a schedule has changed
    after_schedule_change(session)
$$;


CREATE OR REPLACE PROCEDURE ADMIN.ENABLE_WAREHOUSE_SCHEDULING(warehouse_name text)
    RETURNS TEXT
    LANGUAGE PYTHON
    runtime_version = "3.10"
    handler = 'run'
    packages = ('snowflake-snowpark-python', 'pydantic')
    imports = ('{{stage}}/python/crud.zip')
    EXECUTE AS OWNER
AS
$$
from crud.wh_sched import WarehouseSchedules
def run(session, name: str):
    # Find a matching schedule
    WarehouseSchedules.enable_scheduling(session, name, True)
$$;


CREATE OR REPLACE PROCEDURE ADMIN.DISABLE_WAREHOUSE_SCHEDULING(warehouse_name text)
    RETURNS TEXT
    LANGUAGE PYTHON
    runtime_version = "3.10"
    handler = 'run'
    packages = ('snowflake-snowpark-python', 'pydantic')
    imports = ('{{stage}}/python/crud.zip')
    EXECUTE AS OWNER
AS
$$
from crud.wh_sched import WarehouseSchedules
def run(session, name: str):
    # Find a matching schedule
    WarehouseSchedules.enable_scheduling(session, name, False)
$$;
