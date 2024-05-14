
create or replace procedure admin.finalize_setup()
returns string
language sql
execute as owner
AS
DECLARE
    finalize_start_time timestamp_ltz default (select current_timestamp());
    old_version text;
BEGIN
    -- Get the old version of the native app
    call internal.get_config('post_setup') into :old_version;

    -- These can't be created until after IMPORTED PRIVILEGES ON SNOWFLAKE DB is granted to application.
    BEGIN
        DECLARE
          c1 CURSOR FOR
              with columns as (
                select table_schema, table_name, LISTAGG('"' || COLUMN_NAME || '" ', ', \n') WITHIN GROUP (ORDER BY ORDINAL_POSITION) as cols
                from snowflake.information_schema.columns
                where table_catalog = 'SNOWFLAKE' and table_schema in ('ACCOUNT_USAGE', 'ORGANIZATION_USAGE')
                group by table_name, table_schema
              ), delays as (
            select $1 as table_name, $2 as delay, $3 as ts from (values
                ('QUERY_HISTORY', 180, 'END_TIME'),
                ('WAREHOUSE_EVENTS_HISTORY', 180, 'TIMESTAMP'),
                ('WAREHOUSE_LOAD_HISTORY', 180, 'END_TIME'),
                ('WAREHOUSE_METERING_HISTORY', 360, 'END_TIME'),
                ('USERS', 120, 'CREATED_ON'),
                ('SERVERLESS_TASK_HISTORY', 180, 'END_TIME'),
                ('TASK_HISTORY', 180, 'COMPLETED_TIME'),
                ('SESSIONS', 180, 'CREATED_ON'),
                ('TAGS', 0, ''),
                ('TAG_REFERENCES', 0, ''),
                ('OBJECT_DEPENDENCIES', 0, ''),
                ('MATERIALIZED_VIEW_REFRESH_HISTORY', 180, 'END_TIME'),
                ('HYBRID_TABLE_USAGE_HISTORY', 180, 'END_TIME'),
                ('HYBRID_TABLES', 0, ''),
                ('TABLE_STORAGE_METRICS', 0, ''),
                ('LOGIN_HISTORY', 120, 'EVENT_TIMESTAMP')
            )
            )
              select case when d.delay > 0 then
                 'create or replace view "' || v.table_schema || '"."' || v.table_name || '" AS select '|| c.cols || ' from "' || v.table_catalog || '"."' || v.table_schema || '"."' || v.table_name ||'" WHERE ' || d.ts || ' < timestampadd(minute, -' || d.delay || ', current_timestamp) '
            else
                'create or replace view "' || v.table_schema || '"."' || v.table_name || '" AS select '|| c.cols || ' from "' || v.table_catalog || '"."' || v.table_schema || '"."' || v.table_name ||'"'
            end
                  as v
              from snowflake.information_schema.views v
              join columns c on v.table_schema = c.table_schema and v.table_name = c.table_name
              left outer join delays d on v.table_name = d.table_name
              where v.table_catalog = 'SNOWFLAKE' AND v.table_schema in ('ACCOUNT_USAGE') AND v.table_name in ('QUERY_HISTORY', 'WAREHOUSE_EVENTS_HISTORY', 'WAREHOUSE_LOAD_HISTORY', 'WAREHOUSE_METERING_HISTORY', 'USERS', 'SERVERLESS_TASK_HISTORY', 'TASK_HISTORY', 'SESSIONS', 'LOGIN_HISTORY', 'TABLE_STORAGE_METRICS', 'HYBRID_TABLES', 'HYBRID_TABLE_USAGE_HISTORY', 'MATERIALIZED_VIEW_REFRESH_HISTORY', 'OBJECT_DEPENDENCIOES', 'TAG_REFERENCES', 'TAGS');
          counter int := 0;
        BEGIN
            FOR record IN c1 DO
                EXECUTE IMMEDIATE record.v;
                counter := counter + 1;
            END FOR;
        END;
    END;

-- Re-generate the internal and public views. These require access to the SNOWFLAKE database.
call internal.migrate_queries();
call internal.migrate_warehouse_events();
call internal.migrate_view();

-- These can't be created until after EXECUTE MANAGED TASK is granted to application.
CREATE OR REPLACE TASK TASKS.WAREHOUSE_EVENTS_MAINTENANCE
    SCHEDULE = '60 minute'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = "XSMALL"
    AS
DECLARE
    root_task_id text default (select INTERNAL.ROOT_TASK_ID());
    task_run_id text default (select INTERNAL.TASK_RUN_ID());
    task_name text default 'WAREHOUSE_EVENTS_MAINTENANCE';
    object_name text default 'WAREHOUSE_EVENTS_HISTORY';
BEGIN
    let query_id text := (select query_id from table(information_schema.task_history(TASK_NAME => :task_name, ROOT_TASK_ID => :root_task_id)) WHERE GRAPH_RUN_GROUP_ID = :task_run_id  AND DATABASE_NAME = current_database() limit 1);

    -- One-time migration from the old table to the consolidated task_log
    if (exists (select * from information_schema.tables where table_schema = 'INTERNAL' and table_name = 'TASK_WAREHOUSE_EVENTS')) then
        BEGIN TRANSACTION;
        let config_key := (select 'MIGRATION_TASK_WAREHOUSE_EVENTS');
        let config_value text;
        call internal.get_config(:config_key) into :config_value;
        if (config_value is null) then
            -- Copy all materializations from the old table to the new table for WAREHOUSE_EVENTS_HISTORY so the task finds its input/output properly
            INSERT INTO INTERNAL.TASK_LOG (task_start, success, task_name, object_name, input, output, task_finish)
                SELECT twe.run, twe.success, 'WAREHOUSE_EVENTS_MAINTENANCE', 'WAREHOUSE_EVENTS_HISTORY', twe.input, twe.output, twe.run,
                FROM INTERNAL.TASK_WAREHOUSE_EVENTS twe;
            -- Copy any materializations for CLUSTER_SESSIONS and WAREHOUSE_SESSIONS so the materialization_status view will report these two object_names correctly
            insert into INTERNAL.TASK_LOG(task_start, success, task_name, object_name, input, output, task_finish, task_run_id, query_id, range_min, range_max)
                select task_start, success, task_name, 'CLUSTER_SESSIONS', input, output, task_finish, task_run_id, query_id, range_min, range_max from INTERNAL.TASK_LOG
                where task_name = 'WAREHOUSE_EVENTS_MAINTENANCE' and OBJECT_NAME = 'WAREHOUSE_EVENTS_HISTORY';
            insert into INTERNAL.TASK_LOG(task_start, success, task_name, object_name, input, output, task_finish, task_run_id, query_id, range_min, range_max)
                select task_start, success, task_name, 'WAREHOUSE_SESSIONS', input, output, task_finish, task_run_id, query_id, range_min, range_max from INTERNAL.TASK_LOG
                where task_name = 'WAREHOUSE_EVENTS_MAINTENANCE' and OBJECT_NAME = 'WAREHOUSE_EVENTS_HISTORY';

            call internal.set_config(:config_key, 'true');
        END IF;
        commit;
    end if;


    let input object;
    CALL INTERNAL.START_TASK(:task_name, :object_name, :task_run_id, :query_id) into :input;

    let output variant;
    CALL INTERNAL.refresh_warehouse_events(true, :input) into :output;

    CALL INTERNAL.FINISH_TASK(:task_name, :object_name, :task_run_id, :output);

    -- Warehouse events history is a special case where we want to distinctly track warehouse sessions and cluster sessions
    -- but not change the existing materialization logic which materializes both at the same time.
    -- We create a task-level row using the typical API above, but then create domain-specific rows below.
    CALL INTERNAL.FINISH_WAREHOUSE_EVENTS_TASK(:task_name, :task_run_id, :output);
END;

CREATE OR REPLACE TASK TASKS.SIMPLE_DATA_EVENTS_MAINTENANCE
    SCHEDULE = '720 minute' -- 12hours
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = "XSMALL"
    AS
DECLARE
    task_name text default 'SIMPLE_DATA_EVENTS_MAINTENANCE';
    root_task_id text default (select INTERNAL.ROOT_TASK_ID());
    task_run_id text default (select INTERNAL.TASK_RUN_ID());
BEGIN
    let query_id text := (select query_id from table(information_schema.task_history(TASK_NAME => :task_name, ROOT_TASK_ID => :root_task_id)) WHERE GRAPH_RUN_GROUP_ID = :task_run_id  AND DATABASE_NAME = current_database() limit 1);

    -- One-time migration from the old table to the consolidated task_log
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


    let simple_tables resultset := (SELECT t.table_name, t.index_col FROM (VALUES
        ('SERVERLESS_TASK_HISTORY', 'end_time'),
        ('TASK_HISTORY', 'completed_time'),
        ('SESSIONS', 'created_on'),
        ('WAREHOUSE_METERING_HISTORY', 'end_time'),
        ('LOGIN_HISTORY', 'event_timestamp'),
        ('HYBRID_TABLE_USAGE_HISTORY', 'end_time'),
        ('MATERIALIZED_VIEW_REFRESH_HISTORY', 'end_time')
    ) AS t(table_name, index_col));
    let cur cursor for simple_tables;
    FOR rowvar IN cur DO
        let table_name text := rowvar.table_name;
        let index_col text := rowvar.index_col;
        BEGIN
            let input object;
            CALL INTERNAL.START_TASK(:task_name, :table_name, :task_run_id, :query_id) into input;

            let output variant;
            CALL INTERNAL.refresh_simple_table(:table_name, :index_col, true, :input) into :output;

            CALL INTERNAL.FINISH_TASK(:task_name, :table_name, :task_run_id, :output);
        END;
    END FOR;
END;

CREATE OR REPLACE TASK TASKS.QUERY_HISTORY_MAINTENANCE
    SCHEDULE = '60 minute'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = "LARGE"
    AS
DECLARE
    task_name text default 'QUERY_HISTORY_MAINTENANCE';
    object_name text default 'QUERY_HISTORY';
    root_task_id text default (select INTERNAL.ROOT_TASK_ID());
    task_run_id text default (select INTERNAL.TASK_RUN_ID());
BEGIN
    let query_id text := (select query_id from table(information_schema.task_history(TASK_NAME => :task_name, ROOT_TASK_ID => :root_task_id)) WHERE GRAPH_RUN_GROUP_ID = :task_run_id  AND DATABASE_NAME = current_database() limit 1);

    -- One-time migration from the old table to the consolidated task_log
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

    let input object;
    CALL INTERNAL.START_TASK(:task_name, :object_name, :task_run_id, :query_id) into :input;

    let output object;
    CALL INTERNAL.refresh_queries(true, :input) into :output;

    CALL INTERNAL.FINISH_TASK(:task_name, :object_name, :task_run_id, :output);
END;

CREATE OR REPLACE TASK TASKS.SFUSER_MAINTENANCE
    SCHEDULE = '1440 minute'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = "XSMALL"
    AS
    CALL INTERNAL.refresh_users();

CREATE OR REPLACE TASK TASKS.UPGRADE_CHECK
    SCHEDULE = '1440 minute'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = "XSMALL"
    AS
    CALL ADMIN.UPGRADE_CHECK();

CREATE OR REPLACE TASK TASKS.PROBE_MONITORING
    SCHEDULE = '1 minute'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = "XSMALL"
    AS
DECLARE
  sql STRING;
  identifier STRING := (select current_account());
BEGIN
    SYSTEM$LOG_DEBUG('probe_monitoring task beginning');
    CALL INTERNAL.GET_PROBE_SELECT() into :sql;
    execute immediate sql;
    LET c1 CURSOR FOR SELECT probe_name, query_id, to_json(action_taken) as action_takens, action_taken, query_text, user_name, warehouse_name, start_time FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    FOR act IN c1 DO
        let outcome string := '';
        if (act.action_taken:CANCEL::boolean) then
            let id string := act.query_id;
            select SYSTEM$CANCEL_QUERY(:id);
            BEGIN
                outcome := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
            EXCEPTION
                when EXPRESSION_ERROR THEN
                    outcome := SQLERRM;
                WHEN other THEN
                    outcome := SQLERRM;
            END;
        end if;

        -- Send emails
        let emails string := act.action_taken:EMAIL;
        if (length(emails) > 1) then
            let subject string := 'Sundeck OpsCenter probe [{probe_name}] matched query.';
            let body string := '
                Query Id: {query_id}\n
                Query User: {user_name}\n
                Warehouse Name: {warehouse_name}\n
                Start Time: {start_time}\n
                Query Text: \n{query_text}\n
                Account Locator:\n{identifier}\n
            ';
            let dict variant := OBJECT_CONSTRUCT('query_id', act.query_id, 'query_text', act.query_text, 'user_name', act.user_name, 'warehouse_name', act.warehouse_name, 'start_time', act.start_time, 'probe_name', act.probe_name, 'identifier', :identifier);
            let outcome2 string := '';
            BEGIN
                outcome2 := (select to_json(INTERNAL.NOTIFICATIONS(tools.templatejs(:body, :dict), tools.templatejs(:subject, :dict), 'email', :emails)));
            EXCEPTION
                when EXPRESSION_ERROR THEN
                    outcome2 := SQLERRM;
                WHEN other THEN
                    outcome2 := SQLERRM;
            END;
            outcome := (select :outcome || coalesce(:outcome2, ''));
        end if;

        -- Send slack messages
        let slackDests string := act.action_taken:SLACK;
        IF (length(slackDests) > 1) THEN
            let slackResult string := '';
            let dict variant := OBJECT_CONSTRUCT('bt', CHAR(UNICODE('\u0060')), 'query_id', act.query_id, 'query_text', act.query_text, 'user_name', act.user_name, 'warehouse_name', act.warehouse_name, 'start_time', act.start_time, 'probe_name', act.probe_name, 'identifier', :identifier);
            let message string := '
Sundeck OpsCenter probe [{probe_name}] matched query.\n
Query Id: {bt}{query_id}{bt}\n
Query User: {user_name}\n
Warehouse Name: {bt}{warehouse_name}{bt}\n
Start Time: {bt}{start_time}{bt}\n
Query Text: {bt}{bt}{bt}{query_text}{bt}{bt}{bt}\n
Account Locator: {bt}{identifier}{bt}\n
            ';
           BEGIN
               slackResult := (select to_json(INTERNAL.NOTIFICATIONS(tools.templatejs(:message, :dict), 'unused', 'slack', :slackDests)));
           EXCEPTION
           WHEN other THEN
               slackResult := SQLERRM;
           END;
            outcome := (select :outcome || coalesce(:slackResult, ''));
       END IF;

       let name string := act.PROBE_NAME;
       let action string := act.ACTION_TAKENS;
       let query_id string := act.QUERY_ID;
       insert into internal.probe_actions select CURRENT_TIMESTAMP(), :name, :query_id, parse_json(:action), :outcome;
   END FOR;
EXCEPTION
   WHEN OTHER THEN
       SYSTEM$LOG_ERROR(OBJECT_CONSTRUCT('error', 'Unhandled exception occurred during probe monitoring.', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate));
       insert into internal.probe_actions select CURRENT_TIMESTAMP(), '', '', null::VARIANT, 'Caught unhandled exception: ' || :sqlerrm;
       RAISE;
END;

CREATE OR REPLACE TASK TASKS.USER_LIMITS_MAINTENANCE
    SCHEDULE = '5 minute'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = "XSMALL"
    AS
DECLARE
    start_time timestamp_ltz default (select current_timestamp());
    qh_func_start_time timestamp_ltz default (select internal.get_query_history_func_start_range(:start_time));
BEGIN
    SYSTEM$LOG_DEBUG('starting task to monitor user and role cost');

    -- Read account_usage.query_history and summarize by user/role and write to the table
    MERGE INTO internal.aggregated_hourly_quota q
    USING (
        with todays_queries as (
            select total_elapsed_time,
                   credits_used_cloud_services,
                   warehouse_size,
                   user_name,
                   role_name,
                   end_time,
                   warehouse_name
            from table (information_schema.query_history(
                   RESULT_LIMIT => 10000,
                   END_TIME_RANGE_START => :qh_func_start_time,
                   -- Critical, else in-progress queries are included
                   END_TIME_RANGE_END => current_timestamp()
                   ))
        ), costed_queries as (
            select
                greatest(0, total_elapsed_time) * internal.warehouse_credits_per_milli(warehouse_size) +
                credits_used_cloud_services as credits_used,
                user_name,
                role_name,
                end_time
            from todays_queries
            WHERE NOT INTERNAL.IS_SERVERLESS_WAREHOUSE(warehouse_name)
        ), usage as (
            select
                user_name                   as name,
                sum(credits_used)           as credits_used,
                date_trunc('day', end_time) as day,
                extract(HOUR from end_time) as hour_of_day,
                'user'                      as persona
            from costed_queries
            group by user_name, day, hour_of_day
            union all
            select role_name                   as name,
                sum(credits_used)           as credits_used,
                date_trunc('day', end_time) as day,
                extract(HOUR from end_time) as hour_of_day,
                'role'                      as persona
            from costed_queries
            group by role_name, day, hour_of_day
        ) select name, credits_used, day, hour_of_day, persona from usage
    ) new_usage (name, credits_used, day, hour_of_day, persona)
    -- Name + persona + date + hour, e.g. (josh, user, 2023-08-24, 12)
    ON q.name = new_usage.name AND q.persona = new_usage.persona AND q.day = new_usage.day AND q.hour_of_day = new_usage.hour_of_day
    WHEN MATCHED THEN
        -- Set the new credits_used as we recomputed that whole hour
        UPDATE SET q.credits_used = new_usage.credits_used, q.last_updated = :start_time
    WHEN NOT MATCHED THEN
        INSERT (name, credits_used, day, hour_of_day, persona, last_updated)
        VALUES(new_usage.name, new_usage.credits_used, new_usage.day, new_usage.hour_of_day, new_usage.persona, :start_time);

    -- Record the number of rows we wrote
    let rows_added integer := SQLROWCOUNT;

    -- Save the output of that query so we can return it from the task to ease debugging.
    let quota_outcome string := 'Updated ' || :rows_added || ' rows in aggregated_hourly_quota since ' || :qh_func_start_time;

    -- Aggregate the usage for today from aggregated_hourly_quota into the map that the external function expects.
    -- e.g. {'users': {'bob': 1.0}, 'roles': {'PUBLIC': 2.0}}
    let quota_usage object := (
        with todays_usage as (
            select * from internal.aggregated_hourly_quota
            where day = TO_DATE(:start_time)
        ), user_usage as(
            -- Exclude the oldest hourly buckets of cached credit use data as we are getting fresh data
            -- from the information_schema.query_history table function to avoid double-counting.
            SELECT name, sum(credits_used) as credits
            FROM todays_usage
            WHERE persona = 'user'
            group by name
        ), role_usage as (
            SELECT name, sum(credits_used) as credits
            FROM todays_usage
            WHERE persona = 'role'
            group by name
        ), aggr as (
            select 'users' as kind, object_agg(name, credits) as usage_map from user_usage
            union all
            select 'roles' as kind, object_agg(name, credits) as usage_map from role_usage
        )
        select object_agg(kind, usage_map) from aggr);

    -- Send the result to Sundeck, record the outcome from calling the external function
    let ef_outcome string := '';
    BEGIN
        ef_outcome := (select IFNULL(to_json(INTERNAL.REPORT_QUOTA_USED(:quota_usage)), ''));
    EXCEPTION
        WHEN other THEN
            SYSTEM$LOG_ERROR(OBJECT_CONSTRUCT('error', 'External function to report cost usage failed.', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate));
            ef_outcome := 'Failed to report quota usage to Sundeck ' || :sqlerrm;
    END;
    quota_outcome := quota_outcome || '. ' || ef_outcome;

    -- Log the results locally
    INSERT INTO internal.quota_task_history SELECT :start_time, current_timestamp(), :quota_usage, :quota_outcome;

    SYSTEM$LOG_DEBUG('finished cost monitoring');
exception
    when other then
        SYSTEM$LOG_ERROR(OBJECT_CONSTRUCT('error', 'Exception occurred during cost control computation.', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate));
        INSERT INTO internal.quota_task_history SELECT :start_time, current_timestamp(), null, '(' || :sqlcode || ') state=' || :sqlstate || ' msg=' || :sqlerrm;
        RAISE;
END;

CREATE OR REPLACE TASK TASKS.WAREHOUSE_LOAD_MAINTENANCE
    SCHEDULE = '420 minute' -- 7 hours
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = "XSMALL"
    AS
DECLARE
    task_start text default (select current_timestamp()::TEXT);
    task_name text default 'WAREHOUSE_LOAD_MAINTENANCE';
    root_task_id text default (select INTERNAL.ROOT_TASK_ID());
    task_run_id text default (select INTERNAL.TASK_RUN_ID());
BEGIN
    let query_id text := (select query_id from table(information_schema.task_history(TASK_NAME => :task_name, ROOT_TASK_ID => :root_task_id)) WHERE GRAPH_RUN_GROUP_ID = :task_run_id  AND DATABASE_NAME = current_database() limit 1);

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

    -- Refresh the warehouses prior to refreshing the warehouse load history
    call internal.refresh_warehouses();

    let wh resultset := (select name from internal.sfwarehouses);
    let wh_cur cursor for wh;
    for wh_row in wh_cur do
        let wh_name varchar := wh_row.name;
        let output object;
        let input object;

        CALL INTERNAL.START_TASK(:task_name, :wh_name, :task_run_id, :query_id) into :input;

        -- We have to run the warehouse load history query in the task and not in a procedure call by the task. The below block is our "task body".
        begin
            let stmt resultset := (call internal.refresh_one_warehouse_load_history(:wh_name, :input));
            let stmt_cur cursor for stmt;
            let total_inserted_rows number := 0;
            for stmt_row in stmt_cur do
                execute immediate stmt_row.sql;
                -- the only column/row in returned from an insert statement is the number of rows inserted
                let new_inserted_rows number := (select * from TABLE(RESULT_SCAN(LAST_QUERY_ID())));
                total_inserted_rows := total_inserted_rows + new_inserted_rows;
            end for;
            let range_min timestamp := (select min(end_time) from internal_reporting_mv.warehouse_load_history where warehouse_name = :wh_name);
            let new_running timestamp := (select max(end_time) from internal_reporting_mv.warehouse_load_history where warehouse_name = :wh_name);
            output := OBJECT_CONSTRUCT_KEEP_NULL('oldest_running', :new_running, 'new_records', coalesce(:total_inserted_rows, 0), 'range_min', :range_min, 'range_max', :new_running);
        exception
            when other then
                SYSTEM$LOG_ERROR(OBJECT_CONSTRUCT('error', 'Exception occurred while refreshing ' || :wh_name || ' events.', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate));
                output := OBJECT_CONSTRUCT('Error type', 'Other error', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate);
        end;

        CALL INTERNAL.FINISH_TASK(:task_name, :wh_name, :task_run_id, :output);
    end for;

exception
    when other then
        SYSTEM$LOG_ERROR(OBJECT_CONSTRUCT('error', 'Exception occurred while refreshing all warehouse events.', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate));
        insert into INTERNAL.TASK_LOG(task_start, success, object_name, input, output, task_run_id, query_id, task_name) SELECT :task_start, false, 'all', null, OBJECT_CONSTRUCT('Error type', 'Other error', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate)::variant, :task_run_id, :query_id, :task_name;
        RAISE;
end;

-- Create the WAREHOUSE_SCHEDULING task without a schedule only if it doesn't exist.
-- If we CREATE OR REPLACE this task, we will miss scheduling after upgrades because the
-- previous schedule will be overwritten.
CREATE TASK IF NOT EXISTS TASKS.WAREHOUSE_SCHEDULING_0
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = "X-Small"
    AS
    execute task TASKS.WAREHOUSE_SCHEDULING;

CREATE TASK IF NOT EXISTS TASKS.WAREHOUSE_SCHEDULING_15
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = "X-Small"
    AS
    execute task TASKS.WAREHOUSE_SCHEDULING;

CREATE TASK IF NOT EXISTS TASKS.WAREHOUSE_SCHEDULING_30
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = "X-Small"
    AS
    execute task TASKS.WAREHOUSE_SCHEDULING;

CREATE TASK IF NOT EXISTS TASKS.WAREHOUSE_SCHEDULING_45
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = "X-Small"
    AS
    execute task TASKS.WAREHOUSE_SCHEDULING;

-- The real warehouse scheduling logic cannot be in python with serverless task. Make a dummy task body.
CREATE OR REPLACE TASK TASKS.WAREHOUSE_SCHEDULING
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = "X-Small"
    as
    call INTERNAL.UPDATE_WAREHOUSE_SCHEDULES(NULL, NULL);

-- This clarifies that the post setup script has been executed to match the current installed version.
let version string := (select internal.get_version());
call internal.set_config('post_setup', :version);

-- Reapply perms since the tasks were overwritten.
grant MONITOR, OPERATE on all tasks in schema TASKS to APPLICATION ROLE ADMIN;

-- enable tasks
call ADMIN.UPDATE_PROBE_MONITOR_RUNNING();
alter task TASKS.SFUSER_MAINTENANCE resume;
alter task TASKS.WAREHOUSE_EVENTS_MAINTENANCE resume;
alter task TASKS.SIMPLE_DATA_EVENTS_MAINTENANCE resume;
alter task TASKS.QUERY_HISTORY_MAINTENANCE resume;
alter task TASKS.UPGRADE_CHECK resume;
alter task TASKS.WAREHOUSE_LOAD_MAINTENANCE resume;
alter task TASK_QUEUE.RUN_TASKS_TASK resume;
-- Do not enable any warehouse_scheduling tasks. They are programmatically resumed when a warehouse schedule is enabled.

-- Kick off the maintenance tasks.
execute task TASKS.SFUSER_MAINTENANCE;
execute task TASKS.WAREHOUSE_EVENTS_MAINTENANCE;
execute task TASKS.SIMPLE_DATA_EVENTS_MAINTENANCE;
execute task TASKS.QUERY_HISTORY_MAINTENANCE;
execute task TASKS.WAREHOUSE_LOAD_MAINTENANCE;

-- Only enable and start user limits task if connected to sundeck
-- let has_url boolean;
-- let has_tenant_url boolean;
-- call internal.has_config('url') into :has_url;
-- call internal.has_config('tenant_url') into :has_tenant_url;
-- if (:has_url or :has_tenant_url) then
--     call internal.start_user_limits_task();
-- end if;
call internal.maybe_set_config('compute_credit_cost', '2.0');
call internal.maybe_set_config('serverless_credit_cost', '3.0');
call internal.maybe_set_config('storage_cost', '40.0');
call internal.maybe_set_config('default_timezone', 'America/Los_Angeles');

-- Determine if the account has warehouse autoscaling and cache it in the config
let has_autoscaling boolean;
call internal.account_has_autoscaling() into :has_autoscaling;
call internal.set_config('autoscaling_available', :has_autoscaling);
CALL admin.setup_external_functions('opscenter_api_integration');

INSERT INTO internal.upgrade_history SELECT :finalize_start_time, CURRENT_TIMESTAMP(), :old_version, internal.get_version(), 'FINALIZE_SETUP: Success';

EXCEPTION
   WHEN OTHER THEN
       SYSTEM$LOG_ERROR(OBJECT_CONSTRUCT('error', 'Unhandled exception occurred during finalize_setup.', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate));
       INSERT INTO internal.upgrade_history SELECT :finalize_start_time, CURRENT_TIMESTAMP(), :old_version, internal.get_version(), 'FINALIZE_SETUP: (' || :sqlcode || ') state=' || :sqlstate || ' msg=' || :sqlerrm;
       RAISE;
END;

-- Create a table and view for the outcome from ADMIN.FINALIZE_SETUP
CREATE TABLE IF NOT EXISTS INTERNAL.UPGRADE_HISTORY(start_time timestamp_ltz, end_time timestamp_ltz, old_version string, new_version string, outcome string);
CREATE OR REPLACE VIEW REPORTING.UPGRADE_HISTORY AS SELECT * FROM INTERNAL.UPGRADE_HISTORY;
