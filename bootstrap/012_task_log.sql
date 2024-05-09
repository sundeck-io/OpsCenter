
create or replace function internal.root_task_id()
returns text
AS
$$
     SYSTEM$TASK_RUNTIME_INFO('CURRENT_ROOT_TASK_UUID')
$$;

create or replace function internal.task_run_id()
returns text
AS
$$
     SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID')
$$;

-- Inserts a row into TASK_LOG with the time the task started, the name of the object being materialized, the graph run ID and query_id for the task.
create or replace procedure internal.start_task(task_name text, object_name text, task_run_id text, query_id text)
    returns object
    language sql
AS
BEGIN
    let start_time timestamp_ltz := (select current_timestamp());
    let input object := (select output from INTERNAL.TASK_LOG where success AND task_name = :task_name AND object_name = :object_name order by task_start desc limit 1);
    -- Set the previous range_min/max when we start a task to avoid a look-back in admin.materialization_status
    let range_min timestamp_ltz := (select :input['range_min']);
    let range_max timestamp_ltz := (select :input['range_max']);
    INSERT INTO INTERNAL.TASK_LOG(task_start, task_run_id, query_id, input, task_name, object_name, range_min, range_max)
        select :start_time, :task_run_id, :query_id, :input, :task_name, :object_name, :range_min, :range_max;
    return input;
END;

-- Updates the row created by START_TASK with the time the task finish, the success and output object of the task, and the min/max date range of the data that is now materialized.
create or replace procedure internal.finish_task(task_name text, object_name text, task_run_id text, output object)
    returns text
    language sql
AS
BEGIN
    let success boolean := (select :output['SQLERRM'] is null);
    let range_min timestamp_ltz := (select :output['range_min']);
    let range_max timestamp_ltz := (select :output['range_max']);
    UPDATE INTERNAL.TASK_LOG
        SET success = :success, output = :output, task_finish = current_timestamp(), range_min = :range_min, range_max = :range_max
        WHERE task_run_id = :task_run_id AND task_name = :task_name AND object_name = :object_name;
END;

-- Special case for the warehouse events task, which inserts extra rows into task log breaking out warehouse and cluster sessions details to
-- ease reporting on each dataset.
create or replace procedure internal.finish_warehouse_events_task(task_name text, task_run_id text, output object)
    returns text
    language sql
AS
BEGIN
    let task_finish timestamp_ltz := (select current_timestamp());
    let success boolean := (select :output['SQLERRM'] is null);
    let cluster_range_min timestamp_ltz := (select :output['cluster_range_min']);
    let cluster_range_max timestamp_ltz := (select :output['cluster_range_max']);
    INSERT INTO INTERNAL.TASK_LOG(task_start, success, task_name, object_name, input, output, task_finish, task_run_id, query_id, range_min, range_max)
        select task_start, :success, :task_name, 'CLUSTER_SESSIONS', input, :output,
        :task_finish, :task_run_id, query_id, :cluster_range_min, :cluster_range_max
        from internal.task_log
        where task_run_id = :task_run_id and task_name = :task_name and object_name = 'WAREHOUSE_EVENTS_HISTORY';

    let warehouse_range_min timestamp_ltz := (select :output['warehouse_range_min']);
    let warehouse_range_max timestamp_ltz := (select :output['warehouse_range_max']);
    INSERT INTO INTERNAL.TASK_LOG(task_start, success, task_name, object_name, input, output, task_finish, task_run_id, query_id, range_min, range_max)
        select task_start, :success, :task_name, 'WAREHOUSE_SESSIONS', input, :output,
        :task_finish, :task_run_id, query_id, :warehouse_range_min, :warehouse_range_max
        from internal.task_log
        where task_run_id = :task_run_id and task_name = :task_name and object_name = 'WAREHOUSE_EVENTS_HISTORY';
END;

-- Generic table that tasks should record their execution into.
CREATE TABLE INTERNAL.TASK_LOG IF NOT EXISTS (task_start timestamp_ltz, success boolean, task_name varchar, object_name varchar,
    input variant, output variant, task_finish timestamp_ltz, task_run_id text, query_id text, range_min timestamp_ltz, range_max timestamp_ltz);

-- migrate the old, separate "task log" tables into the consolidated task_log table.
create or replace procedure internal.migrate_task_logs()
    returns text
    language sql
AS
BEGIN
    -- If the old task log tables exist, copy the rows into the new task_log table
    if (exists (select * from information_schema.tables where table_schema = 'INTERNAL' and table_name = 'TASK_QUERY_HISTORY')) then
        BEGIN TRANSACTION;
        let config_key := (select 'MIGRATION_TASK_QUERY_HISTORY');
        let config_value text;
        call internal.get_config(:config_key) into :config_value;
        if (config_value is null) then
            INSERT INTO INTERNAL.TASK_LOG (task_start, success, task_name, object_name, input, output, task_finish)
                SELECT tqh.run, tqh.success, 'QUERY_HISTORY_MAINTENANCE', 'QUERY_HISTORY', tqh.input, tqh.output, tqh.run,
                FROM INTERNAL.TASK_QUERY_HISTORY tqh;
            call internal.set_config(:config_key, 'true');
        END IF;
        commit;
    end if;

    if (exists (select * from information_schema.tables where table_schema = 'INTERNAL' and table_name = 'TASK_WAREHOUSE_EVENTS')) then
        BEGIN TRANSACTION;
        let config_key := (select 'MIGRATION_TASK_WAREHOUSE_EVENTS');
        let config_value text;
        call internal.get_config(:config_key) into :config_value;
        if (config_value is null) then
            INSERT INTO INTERNAL.TASK_LOG (task_start, success, task_name, object_name, input, output, task_finish)
                SELECT twe.run, twe.success, 'WAREHOUSE_EVENTS_MAINTENANCE', 'WAREHOUSE_EVENTS_HISTORY', twe.input, twe.output, twe.run,
                FROM INTERNAL.TASK_WAREHOUSE_EVENTS twe;
            call internal.set_config(:config_key, 'true');
        END IF;
        commit;
    end if;

    if (exists (select * from information_schema.tables where table_schema = 'INTERNAL' and table_name = 'TASK_SIMPLE_DATA_EVENTS')) then
        BEGIN TRANSACTION;
        let config_key := (select 'MIGRATION_TASK_SIMPLE_DATA_EVENTS');
        let config_value text;
        call internal.get_config(:config_key) into :config_value;
        if (config_value is null) then
            -- Different from the previous, carrying table_name into task_log as object_name
            INSERT INTO INTERNAL.TASK_LOG (task_start, success, task_name, object_name, input, output, task_finish)
                SELECT tsde.run, tsde.success, 'SIMPLE_DATA_EVENTS_MAINTENANCE', tsde.table_name, tsde.input, tsde.output, tsde.run,
                FROM INTERNAL.TASK_SIMPLE_DATA_EVENTS tsde;
            call internal.set_config(:config_key, 'true');
        END IF;
        commit;
    end if;

    if (exists (select * from information_schema.tables where table_schema = 'INTERNAL' and table_name = 'TASK_WAREHOUSE_LOAD_EVENTS')) then
        BEGIN TRANSACTION;
        let config_key := (select 'MIGRATION_TASK_WAREHOUSE_LOAD_EVENTS');
        let config_value text;
        call internal.get_config(:config_key) into :config_value;
        if (config_value is null) then
            -- Different from the previous, carrying table_name into task_log as object_name
            INSERT INTO INTERNAL.TASK_LOG (task_start, success, task_name, object_name, input, output, task_finish)
                SELECT twle.run, twle.success, 'WAREHOUSE_LOAD_MAINTENANCE', twle.warehouse_name, twle.input, twle.output, twle.run,
                FROM INTERNAL.TASK_WAREHOUSE_LOAD_EVENTS twle;
            call internal.set_config(:config_key, 'true');
        END IF;
        commit;
    end if;

    -- Fill in any null values for task_finish for completed tasks.
    update INTERNAL.TASK_LOG set task_finish = task_start where task_finish is null and success is not null;

    -- Create a FULL materialization row for the objects CLUSTER_SESSIONS and WAREHOUSE_SESSIONS.
    -- We previously did not generate these rows, so we need to backfill the FULL materialization rows. The incremental
    -- materializations will fill themselves on next execution.
    insert into INTERNAL.TASK_LOG(task_start, success, task_name, object_name, input, output, task_finish, task_run_id, query_id, range_min, range_max)
        select task_start, success, task_name, 'CLUSTER_SESSIONS', input, output, task_finish, task_run_id, query_id, range_min, range_max from INTERNAL.TASK_LOG
        where task_name = 'WAREHOUSE_EVENTS_MAINTENANCE' and OBJECT_NAME = 'WAREHOUSE_EVENTS_HISTORY' and input is null;
    insert into INTERNAL.TASK_LOG(task_start, success, task_name, object_name, input, output, task_finish, task_run_id, query_id, range_min, range_max)
        select task_start, success, task_name, 'WAREHOUSE_SESSIONS', input, output, task_finish, task_run_id, query_id, range_min, range_max from INTERNAL.TASK_LOG
        where task_name = 'WAREHOUSE_EVENTS_MAINTENANCE' and OBJECT_NAME = 'WAREHOUSE_EVENTS_HISTORY' and input is null;
END;

-- Try to migrate any previous task log records from the old tables
call internal.migrate_task_logs();

-- Create a generic view for all task executions.
CREATE OR REPLACE VIEW ADMIN.TASK_LOG_HISTORY AS SELECT * FROM INTERNAL.TASK_LOG;

-- Remove view from old location
DROP VIEW IF EXISTS REPORTING.TASK_LOG_HISTORY;
