
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

-- prevent confusion with the overloaded procedure defined below
drop procedure if exists internal.start_task(text, text, text, text);

-- Inserts a row into TASK_LOG with the time the task started, the name of the object being materialized, the graph run ID and query_id for the task.
create or replace procedure internal.start_task(task_name text, object_name text, task_run_id text, query_id text,
        task_log_table text default 'INTERNAL.TASK_LOG', query_history_table text default 'SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY')
    returns object
    language sql
AS
BEGIN
    -- Clean up any outstanding rows for this task that have `success is null`. We are relying on the fact that this procedure is called
    -- when a task begins and all tasks in the NativeApp disallow concurrent execution (else, we may try to update a row for a task still running).
    CALL INTERNAL.CLOSE_LOG_FOR_TASK(:task_log_table, :query_history_table, :task_name, :object_name);

    let input object := (select output from IDENTIFIER(:task_log_table) where success AND task_name = :task_name AND object_name = :object_name order by task_start desc limit 1);
    -- Set the previous range_min/max when we start a task to avoid a look-back in admin.materialization_status
    let range_min timestamp_ltz := (select :input['range_min']);
    let range_max timestamp_ltz := (select :input['range_max']);
    INSERT INTO IDENTIFIER(:task_log_table)(task_start, task_run_id, query_id, input, task_name, object_name, range_min, range_max)
        select current_timestamp(), :task_run_id, :query_id, :input, :task_name, :object_name, :range_min, :range_max;
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

drop procedure if exists internal.migrate_task_logs();

-- Create a generic view for all task executions.
CREATE OR REPLACE VIEW ADMIN.TASK_LOG_HISTORY AS SELECT * FROM INTERNAL.TASK_LOG;

-- Remove view from old location
DROP VIEW IF EXISTS REPORTING.TASK_LOG_HISTORY;

CREATE OR REPLACE PROCEDURE INTERNAL.CLOSE_LOG_FOR_TASK(task_log_table text, query_history_table text, task_name text, object_name text)
    RETURNS OBJECT
    LANGUAGE SQL
    Comment = 'Closes an open task log rows for the given task name and object name. Intended to be called at the start of each task.'
AS
BEGIN
    -- Mark records for the given task_name and object_name that are still "open" (success is null) as having failed.
    update identifier(:task_log_table) task_log
    set success = false, output = details.output, task_finish = current_timestamp()
    from (
        with open_queries as (
            select
                query_id,
                task_name,
                object_name,
                task_run_id,
                'Could not find query history for task=' || task_name || ', object=' || object_name || ', task_run_id=' || task_run_id || ', query_id=' || COALESCE(query_id, 'NULL') as default_msg,
                OBJECT_CONSTRUCT('FORCE_CLOSED', true) as closed_output
            from identifier(:task_log_table) where success is null
        ), open_query_details as (
            -- Limit the query history lookback to one day to avoid pulling back too many rows from the secure view
            select
                query_id,
                object_construct(*) as details
            from identifier(:query_history_table)
            where query_id in (select query_id from open_queries where query_id is not null) and start_time > current_timestamp() - interval '1 day'
        )
        select oq.query_id, oq.task_name, oq.object_name, task_run_id,
            case
                when qd.details is not null then INTERNAL.MERGE_OBJECTS(oq.closed_output, qd.details)
                else OBJECT_INSERT(oq.closed_output, 'details', oq.default_msg)
            end as output,
        from open_queries oq
        left join open_query_details qd on oq.query_id = qd.query_id
    ) as details
    where details.query_id = task_log.query_id and details.task_run_id = task_log.task_run_id and task_log.success is null;

    let o object := (select OBJECT_CONSTRUCT('success', TRUE, 'rows_closed', "number of rows updated") from table(result_scan(last_query_id())));

    SYSTEM$LOG_INFO(:o);
    return o;
EXCEPTION
    WHEN OTHER THEN
        let o object := OBJECT_CONSTRUCT('error', 'Exception trying to close outstanding task rows.', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate);
        SYSTEM$LOG_ERROR(:o);
        return o;
END;
