
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

create or replace procedure internal.start_task(task_name text, object_name text, start_time timestamp_ltz, task_run_id text, query_id text)
    returns object
    language sql
AS
BEGIN
    let input variant := (select output from INTERNAL.TASK_LOG where success AND task_name = :task_name AND object_name = :object_name order by task_start desc limit 1);
    INSERT INTO INTERNAL.TASK_LOG(task_start, task_run_id, query_id, input, task_name, object_name) select :start_time, :task_run_id, :query_id, :input, :task_name, :object_name;
    return input;
END;

create or replace procedure internal.finish_task(task_name text, object_name text, start_time timestamp_ltz, task_run_id text, output object)
    returns text
    language sql
AS
BEGIN
    let success boolean := (select :output['SQLERRM'] is null);
    UPDATE INTERNAL.TASK_LOG SET success = :success, output = :output, task_finish = current_timestamp() WHERE task_start = :start_time AND task_run_id = :task_run_id AND task_name = :task_name AND object_name = :object_name;
END;

-- Generic table that tasks should record their execution into.
CREATE TABLE INTERNAL.TASK_LOG IF NOT EXISTS (task_start timestamp_ltz, success boolean, task_name varchar, object_name varchar,
    input variant, output variant, task_finish timestamp_ltz, task_run_id text, query_id text);

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
        call internal.get_config(config_key) into :config_value;
        if (config_value is null) then
            INSERT INTO INTERNAL.TASK_LOG (task_start, success, task_name, object_name, input, output, task_finish)
                SELECT tqh.run, tqh.success, 'QUERY_HISTORY_MAINTENANCE', 'QUERY_HISTORY', tqh.input, tqh.output, null
                FROM INTERNAL.TASK_QUERY_HISTORY tqh;
            call internal.set_config(config_key, 'true');
        END IF;
        commit;
    end if;

    if (exists (select * from information_schema.tables where table_schema = 'INTERNAL' and table_name = 'TASK_WAREHOUSE_EVENTS')) then
        BEGIN TRANSACTION;
        let config_key := (select 'MIGRATION_TASK_WAREHOUSE_EVENTS');
        let config_value text;
        call internal.get_config(config_key) into :config_value;
        if (config_value is null) then
            INSERT INTO INTERNAL.TASK_LOG (task_start, success, task_name, object_name, input, output, task_finish)
                SELECT twe.run, twe.success, 'WAREHOUSE_EVENTS_MAINTENANCE', 'WAREHOUSE_EVENTS_HISTORY', twe.input, twe.output, null
                FROM INTERNAL.TASK_WAREHOUSE_EVENTS twe;
            call internal.set_config(config_key, 'true');
        END IF;
        commit;
    end if;

    if (exists (select * from information_schema.tables where table_schema = 'INTERNAL' and table_name = 'TASK_SIMPLE_DATA_EVENTS')) then
        BEGIN TRANSACTION;
        let config_key := (select 'MIGRATION_TASK_SIMPLE_DATA_EVENTS');
        let config_value text;
        call internal.get_config(config_key) into :config_value;
        if (config_value is null) then
            -- Different from the previous, carrying table_name into task_log as object_name
            INSERT INTO INTERNAL.TASK_LOG (task_start, success, task_name, object_name, input, output, task_finish)
                SELECT tsde.run, tsde.success, 'SIMPLE_DATA_EVENTS_MAINTENANCE', tsde.table_name, tsde.input, tsde.output, null
                FROM INTERNAL.TASK_SIMPLE_DATA_EVENTS tsde;
            call internal.set_config(config_key, 'true');
        END IF;
        commit;
    end if;
END;

-- Try to migrate any previous task log records from the old tables
call internal.migrate_task_logs();

-- Create specific-purpose views that we had before.
CREATE OR REPLACE VIEW REPORTING.SIMPLE_DATA_EVENTS_TASK_HISTORY AS SELECT * exclude (task_name) rename (object_name as table_name)
    FROM INTERNAL.TASK_LOG WHERE task_name = 'SIMPLE_DATA_EVENTS_MAINTENANCE';
CREATE OR REPLACE VIEW REPORTING.WAREHOUSE_LOAD_EVENTS_TASK_HISTORY AS SELECT * exclude (task_name) rename (object_name as warehouse_name)
    FROM INTERNAL.TASK_LOG where task_name = 'WAREHOUSE_LOAD_MAINTENANCE';
CREATE OR REPLACE VIEW REPORTING.UPGRADE_HISTORY AS SELECT task_start, success, output['old_version'] as old_version, output['new_version'] as new_version, task_finish, task_run_id, query_id,
    FROM INTERNAL.TASK_LOG where task_name = 'UPGRADE_CHECK';

-- Create a generic view for all task executions.
CREATE OR REPLACE VIEW REPORTING.TASK_LOG_HISTORY AS SELECT * FROM INTERNAL.TASK_LOG;
