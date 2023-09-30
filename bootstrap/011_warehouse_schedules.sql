
-- NB. Table is created in 090_post_setup.sql because you cannot call python procs from the setup.sql

-- CREATE TABLE IF NOT EXISTS internal.task_warehouse_schedule(RUN TIMESTAMP_LTZ(9), SUCCESS BOOLEAN, OUTPUT VARIANT)

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
