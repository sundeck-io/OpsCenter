
CREATE TABLE internal.sfwarehouses IF NOT EXISTS (name string, size string);

CREATE OR REPLACE PROCEDURE internal.refresh_warehouses() RETURNS STRING LANGUAGE SQL AS
BEGIN
    BEGIN TRANSACTION;
        truncate table internal.sfwarehouses;
        show warehouses;
        insert into internal.sfwarehouses select "name", "size" from table(result_scan(last_query_id()));
        call internal.set_config('SNOWFLAKE_WAREHOUSE_MAINTENANCE', current_timestamp()::string);
    COMMIT;
END;

CREATE TABLE INTERNAL.TASK_SIMPLE_DATA_EVENTS IF NOT EXISTS (run timestamp, success boolean, table_name varchar, input variant, output variant);

CREATE OR REPLACE PROCEDURE internal.migrate_events(table_name varchar)
returns variant
language sql
as
begin
    SYSTEM$LOG_TRACE('Migrating ' || :table_name || ' data.');
    let migrate1 variant := null;
    call internal.migrate_if_necessary('ACCOUNT_USAGE', :table_name, 'INTERNAL_REPORTING_MV', :table_name) into migrate1;
    call internal.migrate_simple_data_view(:table_name);
    return object_construct('migrate1', migrate1);
end;

CREATE OR REPLACE PROCEDURE internal.refresh_simple_table(table_name varchar, index_col varchar, migrate boolean) RETURNS STRING LANGUAGE SQL
    COMMENT = 'Refreshes the materialized view for a given table. If migrate is true, then the materialized view will be migrated if necessary.'
    AS
BEGIN
    let dt timestamp := current_timestamp();
    SYSTEM$LOG_INFO('Starting refresh ' || :table_name || ' events.');
    let migrate1 string := null;
    if (migrate) then
        let migrate_result variant;
        call internal.migrate_events(:table_name) into migrate_result;
        migrate1 := migrate_result:migrate1::string;
    end if;

    let input variant := null;
    BEGIN
        BEGIN TRANSACTION;
        input := (select output from INTERNAL.TASK_SIMPLE_DATA_EVENTS where success and table_name = :table_name order by run desc limit 1);
        let oldest_running timestamp := 0::timestamp;

        if (input is not null) then
            oldest_running := coalesce(input:oldest_running::timestamp, 0::timestamp);
        end if;

        let table_ident varchar := (select 'INTERNAL_REPORTING_MV.' || :table_name);
        if (oldest_running = 0::timestamp) then
          -- we should ensure that there are no records in the table if this is the first run. This allows a separate process to insert a "reset" message in the log which will cause us to start over again.
          truncate table identifier(:table_ident);
        end if;

        -- if there are incomplete queries, find the min timestamp of the incomplete queries. If there are no incomplete, find the newest timestamp for a filter condition next time.
        let where_clause_complete varchar := (select :index_col || ' >= to_timestamp_ltz(\'' || :oldest_running || '\')');
        let new_closed number;
        call internal.generate_insert_statement('INTERNAL_REPORTING_MV', :table_name, 'ACCOUNT_USAGE', :table_name, :where_clause_complete) into :new_closed;
        let new_running timestamp := (select max(identifier(:index_col)) from identifier(:table_ident));
        insert into INTERNAL.TASK_SIMPLE_DATA_EVENTS SELECT :dt, true, :table_name, :input, OBJECT_CONSTRUCT('oldest_running', :new_running, 'attempted_migrate', :migrate, 'new_records', coalesce(:new_closed, 0))::VARIANT;
        COMMIT;

    EXCEPTION
      WHEN OTHER THEN
        SYSTEM$LOG_ERROR(OBJECT_CONSTRUCT('error', 'Exception occurred while refreshing ' || :table_name || ' events.', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate));
        ROLLBACK;
        insert into INTERNAL.TASK_SIMPLE_DATA_EVENTS SELECT :dt, false, :table_name, :input, OBJECT_CONSTRUCT('Error type', 'Other error', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate)::variant;
        RAISE;

    END;
end;

create or replace procedure internal.migrate_simple_data_view(table_name varchar) returns variant language sql as
begin
    let tbl varchar := 'INTERNAL_REPORTING_MV.' || :table_name;
    let vw varchar := 'REPORTING.' || :table_name;
    create or replace view identifier(:vw) as select * from identifier(:tbl);
end;

create table if not exists internal_reporting_mv.serverless_task_history as select * from account_usage.serverless_task_history where 1=0;
create table if not exists internal_reporting_mv.task_history as select * from account_usage.task_history where 1=0;
create table if not exists internal_reporting_mv.sessions as select * from account_usage.sessions where 1=0;
create table if not exists internal_reporting_mv.warehouse_metering_history as select * from account_usage.warehouse_metering_history where 1=0;
create or replace view reporting.serverless_task_history as select * from internal_reporting_mv.serverless_task_history;
create or replace view reporting.task_history as select * from internal_reporting_mv.task_history;
create or replace view reporting.sessions as select * from internal_reporting_mv.sessions;
create or replace view reporting.warehouse_metering_history as select * from internal_reporting_mv.warehouse_metering_history;
create or replace procedure internal.refresh_all_simple_tables() returns string language sql as
begin
    call internal.refresh_simple_table('SERVERLESS_TASK_HISTORY', 'end_time', true);
    call internal.refresh_simple_table('TASK_HISTORY', 'completed_time', true);
    call internal.refresh_simple_table('SESSIONS', 'created_on', true);
    call internal.refresh_simple_table('WAREHOUSE_METERING_HISTORY', 'end_time', true);
    call internal.refresh_warehouses();
    return 'success';
end;
