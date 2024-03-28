
-- Create table for warehouse schedules
CREATE TABLE INTERNAL.WH_SCHEDULES IF NOT EXISTS(
    id_val text,
    name text,
    start_at time,
    finish_at time,
    size text,
    suspend_minutes number,
    resume boolean,
    scale_min number,
    scale_max number,
    warehouse_mode text,
    comment text,
    weekday boolean,
    day text,
    enabled boolean,
    last_modified timestamp_ltz
);

-- Create table for the outcome of executing warehouse schedules
CREATE TABLE IF NOT EXISTS internal.task_warehouse_schedule(
    run timestamp_ltz,
    success boolean,
    output variant
);

-- Table for the alter statements to match the warehouse schedule definition
CREATE TABLE IF NOT EXISTS internal.warehouse_alter_statements(
    id_val text,
    alter_statement text
);

-- Catalog view for warehouse_schedules
CREATE OR REPLACE VIEW catalog.warehouse_schedules AS
    SELECT * exclude (id_val, day) FROM internal.wh_schedules;

-- Reporting view for the actions taken by warehouse schedules
CREATE OR REPLACE VIEW reporting.warehouse_schedules_task_history as
    SELECT run, success, output:"statements"::ARRAY as statements_executed,
    output:"opscenter timezone"::TEXT as schedule_timezone,
    output:"warehouses_updated"::NUMBER as warehouses_updated
    from internal.task_warehouse_schedule;

-- Inadvertently created by python crud.
drop view if exists catalog.wh_schedules;

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
    packages = ('snowflake-snowpark-python', 'pydantic==1.*', 'snowflake-telemetry-python')
    imports = ('{{stage}}/python/crud.zip')
    EXECUTE AS OWNER
AS
$$
import datetime
import uuid
from crud.base import transaction
from crud.wh_sched import WarehouseSchedules, after_schedule_change, fetch_schedules_with_defaults, merge_new_schedule, verify_and_clean
def create_warehouse_schedule(bare_session, name: str, size: str, start: datetime.time, finish: datetime.time, weekday: bool, suspend_minutes: int, autoscale_mode: str, autoscale_min: int, autoscale_max: int, auto_resume: bool, comment: str):
    with transaction(bare_session) as session:
        # Make sure the default schedule are created.
        _ = fetch_schedules_with_defaults(session, name)

        current_scheds = WarehouseSchedules.find_all_with_weekday(session, name, weekday)

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
            user_modified=True,
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
    packages = ('snowflake-snowpark-python', 'pydantic==1.*', 'snowflake-telemetry-python')
    imports = ('{{stage}}/python/crud.zip')
    EXECUTE AS OWNER
AS
$$
import datetime
from crud.base import transaction
from crud.wh_sched import WarehouseSchedules, after_schedule_change, delete_warehouse_schedule
def run_delete(bare_session, name: str, start: datetime.time, finish: datetime.time, is_weekday: bool):
    with transaction(bare_session) as session:
        # Find the matching schedule
        row = WarehouseSchedules.find_one(session, name, start, finish, is_weekday)
        if not row:
            raise Exception(f"Could not find warehouse schedule: {name}, {start}, {finish}, {'weekday' if is_weekday else 'weekend'}")

        to_delete = WarehouseSchedules.construct(id_val = row.id_val)
        current_scheds = WarehouseSchedules.batch_read(session, sortby="start_at", filter=lambda df: ((df.name == name) & (df.weekday == is_weekday)))
        new_scheds = delete_warehouse_schedule(to_delete, current_scheds)

        # Delete that schedule, leaving a hole
        to_delete.delete(session)

        # Run the updates, filling the hole
        [i.update(session, i) for i in new_scheds]

        # Twiddle the task state after adding a new schedule
        after_schedule_change(session)
$$;


CREATE OR REPLACE PROCEDURE ADMIN.UPDATE_WAREHOUSE_SCHEDULE(warehouse_name text, start_at time, finish_at time, weekday boolean, new_start_at time, new_finish_at time, size text, suspend_minutes number, autoscale_mode text, autoscale_min number, autoscale_max number, auto_resume boolean, comment text)
    RETURNS TEXT
    LANGUAGE PYTHON
    runtime_version = "3.10"
    handler = 'update_warehouse_schedule'
    packages = ('snowflake-snowpark-python', 'pydantic==1.*', 'snowflake-telemetry-python')
    imports = ('{{stage}}/python/crud.zip')
    EXECUTE AS OWNER
AS
$$
import datetime
from crud.base import transaction
from crud.wh_sched import WarehouseSchedules, after_schedule_change, fetch_schedules_with_defaults, update_existing_schedule
def update_warehouse_schedule(bare_session, name: str, start: datetime.time, finish: datetime.time, is_weekday: bool, new_start_at: datetime.time, new_finish_at: datetime.time, size: str, suspend_minutes: int, autoscale_mode: str, autoscale_min: int, autoscale_max: int, auto_resume: bool, comment: str):
    with transaction(bare_session) as session:
        # Make sure the default schedule are created.
        _ = fetch_schedules_with_defaults(session, name)

        # Find a matching schedule
        old_schedule = WarehouseSchedules.find_one(session, name, start, finish, is_weekday)
        if not old_schedule:
            raise Exception(f"Could not find warehouse schedule: {name}, {start}, {finish}, {'weekday' if is_weekday else 'weekend'}")

        # Make the new version of that schedule with the same id_val
        new_schedule = WarehouseSchedules.parse_obj(dict(
            id_val=old_schedule.id_val,
            name=name,
            size=size,
            start_at=new_start_at,
            finish_at=new_finish_at,
            suspend_minutes=suspend_minutes,
            warehouse_mode=autoscale_mode,
            scale_min=autoscale_min,
            scale_max=autoscale_max,
            resume=auto_resume,
            comment=comment,
            enabled=old_schedule.enabled,
            user_modified=True,
        ))

        # Read the current schedules
        schedules = WarehouseSchedules.find_all_with_weekday(session, name, new_schedule.weekday)

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
    packages = ('snowflake-snowpark-python', 'pydantic==1.*', 'snowflake-telemetry-python')
    imports = ('{{stage}}/python/crud.zip')
    EXECUTE AS OWNER
AS
$$
from crud.base import transaction
from crud.wh_sched import WarehouseSchedules
def run(session, name: str):
    with transaction(session) as txn:
        # Find a matching schedule
        WarehouseSchedules.enable_scheduling(txn, name, True)
$$;


CREATE OR REPLACE PROCEDURE ADMIN.DISABLE_WAREHOUSE_SCHEDULING(warehouse_name text)
    RETURNS TEXT
    LANGUAGE PYTHON
    runtime_version = "3.10"
    handler = 'run'
    packages = ('snowflake-snowpark-python', 'pydantic==1.*', 'snowflake-telemetry-python')
    imports = ('{{stage}}/python/crud.zip')
    EXECUTE AS OWNER
AS
$$
from crud.base import transaction
from crud.wh_sched import WarehouseSchedules
def run(session, name: str):
    with transaction(session) as txn:
        # Find a matching schedule
        WarehouseSchedules.enable_scheduling(txn, name, False)
$$;

CREATE OR REPLACE PROCEDURE ADMIN.CREATE_DEFAULT_SCHEDULES(warehouse_name text)
    RETURNS TEXT
    LANGUAGE PYTHON
    runtime_version = "3.10"
    handler = 'run'
    packages = ('snowflake-snowpark-python', 'pydantic==1.*', 'snowflake-telemetry-python')
    imports = ('{{stage}}/python/crud.zip')
    EXECUTE AS OWNER
AS
$$
from crud.wh_sched import fetch_schedules_with_defaults
def run(session, name: str):
    fetch_schedules_with_defaults(session, name)
    return ""
$$;

CREATE OR REPLACE PROCEDURE INTERNAL.MIGRATE_WHSCHED_TABLE()
RETURNS OBJECT
AS
BEGIN
    -- Add user_modified column and set them all to false
    IF (NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'INTERNAL' AND TABLE_NAME = 'WH_SCHEDULES' AND COLUMN_NAME = 'LAST_MODIFIED')) THEN
        ALTER TABLE INTERNAL.WH_SCHEDULES ADD COLUMN LAST_MODIFIED TIMESTAMP_LTZ;
        -- Assume every row currently in the system was modified by a user.
        UPDATE INTERNAL.WH_SCHEDULES SET LAST_MODIFIED = CURRENT_TIMESTAMP();
    END IF;

    CREATE OR REPLACE VIEW catalog.warehouse_schedules COPY GRANTS AS
        SELECT * exclude (id_val, day) FROM internal.wh_schedules;
EXCEPTION
    WHEN OTHER THEN
        SYSTEM$LOG('error', 'Failed to migrate warehouse schedules table. ' || :SQLCODE || ': ' || :SQLERRM);
        raise;
END;

CREATE OR REPLACE PROCEDURE ADMIN.RESET_WAREHOUSE_SCHEDULE(warehouse_name text)
    RETURNS TEXT
    LANGUAGE PYTHON
    runtime_version = "3.10"
    handler = 'run'
    packages = ('snowflake-snowpark-python', 'pydantic==1.*', 'snowflake-telemetry-python')
    imports = ('{{stage}}/python/crud.zip')
    EXECUTE AS OWNER
AS
$$
from crud.base import transaction
from crud.wh_sched import after_schedule_change, fetch_schedules_with_defaults
def run(bare_session, warehouse_name: str):
    with transaction(bare_session) as session:
        session.sql("DELETE FROM internal.wh_schedules WHERE name = ?", params=(warehouse_name,)).collect()

        # Re-create the default schedules for this warehouse
        _ = fetch_schedules_with_defaults(session, warehouse_name)

        # Twiddle the task state after adding a new schedule
        after_schedule_change(session)
        return ""
$$;

CREATE OR REPLACE PROCEDURE INTERNAL.ACCOUNT_HAS_AUTOSCALING()
    RETURNS BOOLEAN
    LANGUAGE SQL
AS
begin
    show warehouses;
    select "min_cluster_count" from table(result_scan(last_query_id()));
    return true;
exception
    when statement_error then
        let missing_column boolean := (select CONTAINS(:SQLERRM, 'invalid identifier') AND CONTAINS(:SQLERRM, 'min_cluster_count'));
        if (missing_column) then
            return false;
        end if;
        raise;
    when other then
        raise;
end;

CREATE OR REPLACE PROCEDURE ADMIN.SHOW_WAREHOUSES()
    RETURNS TABLE(name text, size text)
    LANGUAGE SQL
    execute as owner
AS
BEGIN
    show warehouses;
    let rs resultset := (select "name", "size" from table(result_scan(last_query_id())));
    return table(rs);
END;
