
CREATE TABLE INTERNAL.TASK_QUERY_HISTORY IF NOT EXISTS (run timestamp, success boolean, input variant, output variant);

CREATE OR REPLACE PROCEDURE internal.migrate_queries()
returns variant
language sql
as
begin
    SYSTEM$LOG_TRACE('Migrating query history data.');
    call internal.migrate_view();
    call internal.migrate_if_necessary('INTERNAL_REPORTING', 'QUERY_HISTORY_COMPLETE_AND_DAILY', 'INTERNAL_REPORTING_MV', 'QUERY_HISTORY_COMPLETE_AND_DAILY');
    let migrate1 string := (select * from TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    call internal.migrate_if_necessary('INTERNAL_REPORTING', 'QUERY_HISTORY_COMPLETE_AND_DAILY', 'INTERNAL_REPORTING_MV', 'QUERY_HISTORY_COMPLETE_AND_DAILY_INCOMPLETE');
    let migrate2 string := (select * from TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    return object_construct('migrate1', migrate1, 'migrate2', migrate2);
end;

CREATE OR REPLACE PROCEDURE internal.refresh_queries(migrate boolean) RETURNS STRING LANGUAGE SQL AS
BEGIN
    let dt timestamp := current_timestamp();
    SYSTEM$LOG_INFO('Starting refresh queries.');
    let migrate1 string := null;
    let migrate2 string := null;
    if (migrate) then
        let migration_result variant;
        call internal.migrate_queries() into :migration_result;
        migrate1 := migration_result:migrate1::string;
        migrate2 := migration_result:migrate2::string;
    end if;

    let input variant := null;
    BEGIN
        BEGIN TRANSACTION;
        input := (select output from INTERNAL.TASK_QUERY_HISTORY where success order by run desc limit 1);
        let oldest_running timestamp := 0::timestamp;
        let newest_completed timestamp := 0::timestamp;

        if (input is not null) then
            oldest_running := input:oldest_running::timestamp;
            newest_completed := input:newest_completed::timestamp;
        end if;

        if (oldest_running = 0::timestamp) then
          -- we should ensure that there are no records in the table if this is the first run. This allows a separate process to insert a "reset" message in the log which will cause us to start over again.
          truncate table INTERNAL_REPORTING_MV.QUERY_HISTORY_COMPLETE_AND_DAILY;
        end if;

        DROP TABLE IF EXISTS RAW_QH_EVT ;
        CREATE TABLE RAW_QH_EVT AS SELECT * FROM INTERNAL_REPORTING.QUERY_HISTORY_COMPLETE_AND_DAILY WHERE filterts >= :oldest_running AND end_time >= :newest_completed;
        let new_records number := (select count(*) from RAW_QH_EVT);

        IF (new_records > 0) THEN
            -- if there are incomplete queries, find the min timestamp of the incomplete queries. If there are no incomplete, find the newest timestamp for a filter condition next time.
            oldest_running := (SELECT greatest(coalesce(MIN(case when incomplete then filterts else null end), max(end_time)), :oldest_running) FROM RAW_QH_EVT);
            newest_completed := (SELECT greatest(coalesce(max(end_time), 0::TIMESTAMP), :newest_completed) FROM RAW_QH_EVT WHERE NOT INCOMPLETE);
            let run_id timestamp := (SELECT run_id FROM RAW_QH_EVT limit 1);
            TRUNCATE TABLE INTERNAL_REPORTING_MV.QUERY_HISTORY_COMPLETE_AND_DAILY_INCOMPLETE;
            let where_clause varchar := (select 'INCOMPLETE OR END_TIME = to_timestamp_ltz(\'' || :newest_completed || '\')');
            let new_INCOMPLETE number;
            call internal.generate_insert_statement('INTERNAL_REPORTING_MV', 'QUERY_HISTORY_COMPLETE_AND_DAILY_INCOMPLETE', 'INTERNAL', 'RAW_QH_EVT', :where_clause) into :new_INCOMPLETE;
            let where_clause_complete varchar := (select 'END_TIME <> to_timestamp_ltz(\'' || :newest_completed || '\')');
            let new_closed number;
            call internal.generate_insert_statement('INTERNAL_REPORTING_MV', 'QUERY_HISTORY_COMPLETE_AND_DAILY', 'INTERNAL', 'RAW_QH_EVT', :where_clause_complete) into :new_closed;
            insert into INTERNAL.TASK_QUERY_HISTORY SELECT :run_id, true, :input, OBJECT_CONSTRUCT('oldest_running', :oldest_running, 'newest_completed', :newest_completed, 'attempted_migrate', :migrate, 'migrate', :migrate1, 'migrate_INCOMPLETE', :migrate2, 'new_records', :new_records, 'new_INCOMPLETE', :new_INCOMPLETE, 'new_closed', coalesce(:new_closed, 0))::VARIANT;
        ELSE
            insert into INTERNAL.TASK_QUERY_HISTORY SELECT :dt, true, :input, OBJECT_CONSTRUCT('oldest_running', :oldest_running, 'newest_completed', :newest_completed, 'attempted_migrate', :migrate, 'migrate', :migrate1, 'migrate_INCOMPLETE', :migrate2, 'new_records', 0, 'new_INCOMPLETE', 0, 'new_closed', 0)::VARIANT;
        END IF;
        DROP TABLE RAW_QH_EVT;
        COMMIT;

    EXCEPTION
      WHEN OTHER THEN
        SYSTEM$LOG_ERROR(OBJECT_CONSTRUCT('error', 'Exception occurred while refreshing query history.', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate));
        ROLLBACK;
        insert into INTERNAL.TASK_QUERY_HISTORY SELECT :dt, false, :input, OBJECT_CONSTRUCT('Error type', 'Other error', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate)::variant;
        RAISE;

    END;
    CALL INTERNAL.SET_CONFIG('QUERY_HISTORY_MAINTENANCE', CURRENT_TIMESTAMP()::string);
END;
