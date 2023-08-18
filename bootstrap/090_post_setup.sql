
create or replace procedure admin.finalize_setup()
returns string
language sql
execute as owner
AS
BEGIN
    -- These can't be created until after IMPORTED PRIVILEGES ON SNOWFLAKE DB is granted to application.
    BEGIN
        DECLARE
          c1 CURSOR FOR
              with columns as (
                select table_schema, table_name, LISTAGG('"' || COLUMN_NAME || '" ', ', \n') WITHIN GROUP (ORDER BY ORDINAL_POSITION) as cols
                from information_schema.columns
                where table_catalog = current_database() and table_schema in ('ACCOUNT_USAGE', 'ORGANIZATION_USAGE')
                group by table_name, table_schema
              )
              select
                 'create or replace view "' || v.table_schema || '"."' || v.table_name || '" AS select '|| c.cols || ' from "' || v.table_catalog || '"."' || v.table_schema || '"."' || v.table_name ||
                 '"' as v
              from snowflake.information_schema.views v
              join columns c on v.table_schema = c.table_schema and v.table_name = c.table_name
              where v.table_catalog = 'SNOWFLAKE' AND v.table_schema in ('ACCOUNT_USAGE') AND v.table_name in ('QUERY_HISTORY', 'WAREHOUSE_EVENTS_HISTORY', 'WAREHOUSE_LOAD_HISTORY', 'WAREHOUSE_METERING_HISTORY', 'USERS', 'SERVERLESS_TASK_HISTORY');
          counter int := 0;
        BEGIN
            FOR record IN c1 DO
                EXECUTE IMMEDIATE record.v;
                counter := counter + 1;
            END FOR;
        END;
    END;

-- These can't be created until after EXECUTE MANAGED TASK is granted to application.
CREATE OR REPLACE TASK TASKS.WAREHOUSE_EVENTS_MAINTENANCE
    SCHEDULE = '60 minute'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = "XSMALL"
    AS
    CALL INTERNAL.refresh_warehouse_events(true);

CREATE OR REPLACE TASK TASKS.QUERY_HISTORY_MAINTENANCE
    SCHEDULE = '60 minute'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = "LARGE"
    AS
    CALL INTERNAL.refresh_queries(true);

CREATE OR REPLACE TASK TASKS.SFUSER_MAINTENANCE
    SCHEDULE = '1440 minute'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = "XSMALL"
    AS
    BEGIN
        BEGIN TRANSACTION;
        truncate table internal.sfusers;
        insert into internal.sfusers select name, email from snowflake.account_usage.users;
        COMMIT;
    END;

-- Migrate the schema of the probes table if it already exists
call INTERNAL.MIGRATE_PROBES_TABLE();
call INTERNAL.MIGRATE_LABELS_TABLE();

CREATE OR REPLACE TASK TASKS.PROBE_MONITORING
    SCHEDULE = '1 minute'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = "XSMALL"
    AS
DECLARE
  sql STRING;
BEGIN
    SYSTEM$LOG_DEBUG('probe_monitoring task beginning');
    BEGIN
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
                    Query Text: \n{query_text}
                ';
                let dict variant := OBJECT_CONSTRUCT('query_id', act.query_id, 'query_text', act.query_text, 'user_name', act.user_name, 'warehouse_name', act.warehouse_name, 'start_time', act.start_time, 'probe_name', act.probe_name);
                let outcome2 string := '';
                BEGIN
                    outcome2 := (select to_json(INTERNAL.NOTIFICATIONS(tools.templatejs(:body, :dict), tools.templatejs(:subject, :dict), 'email', :emails)));
                EXCEPTION
                    when EXPRESSION_ERROR THEN
                        outcome2 := SQLERRM;
                    WHEN other THEN
                        outcome2 := SQLERRM;
                END;
                outcome := outcome || outcome2;
            end if;

            -- Send slack messages
            let slackDests string := act.action_taken:SLACK;
            IF (length(slackDests) > 1) THEN
                let slackResult string := '';
                let dict variant := OBJECT_CONSTRUCT('bt', CHAR(UNICODE('\u0060')), 'query_id', act.query_id, 'query_text', act.query_text, 'user_name', act.user_name, 'warehouse_name', act.warehouse_name, 'start_time', act.start_time, 'probe_name', act.probe_name);
                let message string := '
    Sundeck OpsCenter probe [{probe_name}] matched query.\n
    Query Id: {bt}{query_id}{bt}\n
    Query User: {user_name}\n
    Warehouse Name: {bt}{warehouse_name}{bt}\n
    Start Time: {bt}{start_time}{bt}\n
    Query Text: {bt}{bt}{bt}{query_text}{bt}{bt}{bt}
                ';
               BEGIN
                   slackResult := (select to_json(INTERNAL.NOTIFICATIONS(tools.templatejs(:message, :dict), 'unused', 'slack', :slackDests)));
               EXCEPTION
               WHEN other THEN
                   slackResult := SQLERRM;
               END;
               outcome := outcome || slackResult;
           END IF;

           let name string := act.PROBE_NAME;
           let action string := act.ACTION_TAKENS;
           let query_id string := act.QUERY_ID;
           insert into internal.probe_actions select CURRENT_TIMESTAMP(), :name, :query_id, parse_json(:action), :outcome;
       END FOR;
   EXCEPTION
       WHEN OTHER THEN
           SYSTEM$LOG_ERROR(OBJECT_CONSTRUCT('error', 'Unhandled exception occurred during probe monitoring.', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate));
           insert into internal.probe_actions select CURRENT_TIMESTAMP(), '', '', null::VARIANT, 'Caught unhandled exception';
           -- No RAISE to avoid skipping the consumption check
   END;
    SYSTEM$LOG_DEBUG('finished probe_monitoring, check quota usage consumption');

    let generate_consumption boolean := (select internal.is_consumption_enabled());
    if (:generate_consumption) THEN
        SYSTEM$LOG_INFO('starting usage consumption');
        let start_time timestamp_ltz := (select current_timestamp());
        BEGIN
            call internal.get_daily_quota_select() into :sql;
            execute immediate sql;
            let quota_usage object := (select daily_quota from table(result_scan(last_query_id())));
            -- TODO call external function with quota_usage
            insert into internal.quota_task_history select :start_time, current_timestamp(), :quota_usage, 'success';
            SYSTEM$LOG_DEBUG('successfully computed usage consumption');
        exception
            when other then
                SYSTEM$LOG_ERROR(OBJECT_CONSTRUCT('error', 'Exception occurred during quota computation.', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate));
                insert into internal.quota_task_history select :start_time, current_timestamp(), null, '(' || :sqlcode || ') state=' || :sqlstate || ' msg=' || :sqlerrm;
                -- no raise to avoid double-logging
        END;
    END IF;
    SYSTEM$LOG_DEBUG('finished usage consumption, probe_monitoring task is done');
exception
    when other then
        SYSTEM$LOG_ERROR(OBJECT_CONSTRUCT('error', 'unhandled exception in probe_monitoring task', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate));
        RAISE;
END;

-- This clarifies that the post setup script has been executed to match the current installed version.
let version string := (select internal.get_version());
call internal.set_config('post_setup', :version);

-- Reapply perms since the tasks were overwritten.
grant MONITOR, OPERATE on all tasks in schema TASKS to APPLICATION ROLE ADMIN;

-- enable tasks
call ADMIN.UPDATE_PROBE_MONITOR_RUNNING();
alter task TASKS.SFUSER_MAINTENANCE resume;
alter task TASKS.WAREHOUSE_EVENTS_MAINTENANCE resume;
alter task TASKS.QUERY_HISTORY_MAINTENANCE resume;

-- Kick off the maintenance tasks.
execute task TASKS.SFUSER_MAINTENANCE;
execute task TASKS.WAREHOUSE_EVENTS_MAINTENANCE;
execute task TASKS.QUERY_HISTORY_MAINTENANCE;

END;
