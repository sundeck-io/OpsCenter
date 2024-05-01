
CREATE TABLE INTERNAL.WAREHOUSE_SIZE_MAPPING IF NOT EXISTS (WAREHOUSE_NAME varchar, WAREHOUSE_SIZE varchar, WAREHOUSE_TYPE varchar);

CREATE OR REPLACE PROCEDURE INTERNAL.MIGRATE_WAREHOUSE_SIZE_MAPPING()
RETURNS OBJECT
AS
BEGIN
    -- Add WAREHOUSE_TYPE column
    IF (NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'INTERNAL' AND TABLE_NAME = 'WAREHOUSE_SIZE_MAPPING' AND COLUMN_NAME = 'WAREHOUSE_TYPE')) THEN
        ALTER TABLE IF EXISTS INTERNAL.WAREHOUSE_SIZE_MAPPING ADD COLUMN WAREHOUSE_TYPE STRING DEFAULT 'STANDARD';
    END IF;

EXCEPTION
    WHEN OTHER THEN
        SYSTEM$LOG('error', 'Failed to migrate warehouse size mapping table. ' || :SQLCODE || ': ' || :SQLERRM);
        raise;
END;
call internal.migrate_warehouse_size_mapping();

CREATE OR REPLACE PROCEDURE internal.migrate_warehouse_events()
returns variant
language sql
as
begin
    SYSTEM$LOG_TRACE('Migrating warehouse events data.');
    call internal.migrate_if_necessary('INTERNAL_REPORTING', 'CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY', 'INTERNAL_REPORTING_MV', 'CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY');
    let migrate1 variant := (select * from TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    call internal.migrate_if_necessary('INTERNAL_REPORTING', 'CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY', 'INTERNAL_REPORTING_MV', 'CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY_INCOMPLETE');
    let migrate2 variant := (select * from TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    -- Ensure that RECORD_TYPE is VARCHAR and not VARCHAR(8)
    ALTER TABLE INTERNAL_REPORTING_MV.CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY MODIFY COLUMN RECORD_TYPE TYPE VARCHAR;
    ALTER TABLE INTERNAL_REPORTING_MV.CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY_INCOMPLETE MODIFY COLUMN RECORD_TYPE TYPE VARCHAR;
    return object_construct('migrate1', migrate1, 'migrate2', migrate2);
end;

-- input should have fields oldest_running and newest_completed at a minimum
-- output will contain the following entries
--  * oldest_running is a timestamp for the old sessions still running
--  * newest_completed is a timestamp for the newest completed session
--  * range_min and range_max is the minimum and maximum session_end timestamps for the entire materialized dataset at the time of task execution
--  * attempted_migrate will be true if the materialized view was migrated. When true, the 'migrate' key contains migration of the complete MV; the 'migrate_INCOMPLETE' key contains migration of the incomplete MV.
--  * new_records is the number of new records inserted into the complete MV. new_INCOMPLETE is the number of new records inserted into the incomplete MV. new_closed is the number of new records inserted into the complete MV.
-- If an error is created during execution: SQLERRM, SQLCODE, and SQLSTATE will be returned in the output object.
CREATE OR REPLACE PROCEDURE internal.refresh_warehouse_events(migrate boolean, input object) RETURNS OBJECT LANGUAGE SQL
    COMMENT = 'Refreshes the warehouse events materialized view. If migrate is true, then the materialized view will be migrated if necessary.'
    AS
BEGIN
    let dt timestamp := current_timestamp();
    SYSTEM$LOG_INFO('Starting refresh warehouse events.');
    let migrate1 string := null;
    let migrate2 string := null;
    if (migrate) then
        let migrate_result variant;
        call internal.migrate_warehouse_events() into migrate_result;
        migrate1 := migrate_result:migrate1::string;
        migrate2 := migrate_result:migrate2::string;
    end if;

    let output variant := NULL;
    BEGIN
        BEGIN TRANSACTION;
        let oldest_running timestamp := 0::timestamp;
        let newest_completed timestamp := 0::timestamp;

        if (input is not null) then
            oldest_running := input:oldest_running::timestamp;
            newest_completed := input:newest_completed::timestamp;
        end if;

        if (oldest_running = 0::timestamp) then
          -- we should ensure that there are no records in the table if this is the first run. This allows a separate process to insert a "reset" message in the log which will cause us to start over again.
          truncate table INTERNAL_REPORTING_MV.CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY;
        end if;

        DROP TABLE IF EXISTS RAW_WH_EVT ;
        CREATE TABLE RAW_WH_EVT AS SELECT * FROM INTERNAL_REPORTING.CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY WHERE filterts >= :oldest_running AND SESSION_END >= :newest_completed;
        let new_records number := (select count(*) from RAW_WH_EVT);

        IF (new_records > 0) THEN
            -- if there are incomplete queries, find the min timestamp of the incomplete queries. If there are no incomplete, find the newest timestamp for a filter condition next time.
            oldest_running := (SELECT greatest(coalesce(MIN(case when incomplete then filterts else null end), max(SESSION_END)), :oldest_running) FROM RAW_WH_EVT);
            newest_completed := (SELECT greatest(coalesce(max(SESSION_END), 0::TIMESTAMP), :newest_completed) FROM RAW_WH_EVT WHERE NOT INCOMPLETE);
            let run_id timestamp := (SELECT run_id FROM RAW_WH_EVT limit 1);
            TRUNCATE TABLE INTERNAL_REPORTING_MV.CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY_INCOMPLETE;
            let where_clause varchar := (select 'INCOMPLETE OR session_end = to_timestamp_ltz(\'' || :newest_completed || '\')');
            let new_INCOMPLETE number;
            call internal.generate_insert_statement('INTERNAL_REPORTING_MV', 'CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY_INCOMPLETE', 'INTERNAL', 'RAW_WH_EVT', :where_clause) into :new_INCOMPLETE;
            let where_clause_complete varchar := (select 'not incomplete and session_end <> to_timestamp_ltz(\'' || :newest_completed || '\')');
            let new_closed number;
            call internal.generate_insert_statement('INTERNAL_REPORTING_MV', 'CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY', 'INTERNAL', 'RAW_WH_EVT', :where_clause_complete) into :new_closed;
            -- Find oldest session, after the new records have been inserted.
            let range_min timestamp := (select min(session_end) from (
                select min(session_end) as session_end from INTERNAL_REPORTING_MV.CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY
                union all
                select min(session_end) as session_end from INTERNAL_REPORTING_MV.CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY_INCOMPLETE)
            );
            output := OBJECT_CONSTRUCT('oldest_running', :oldest_running, 'newest_completed', :newest_completed, 'attempted_migrate', :migrate, 'migrate', :migrate1, 'migrate_INCOMPLETE', :migrate2,
                'new_records', :new_records, 'new_INCOMPLETE', :new_INCOMPLETE, 'new_closed', coalesce(:new_closed, 0), 'range_min', :range_min, 'range_max', :newest_completed);
        ELSE
            let range_min timestamp := (select min(session_end) from (
                select min(session_end) as session_end from INTERNAL_REPORTING_MV.CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY
                union all
                select min(session_end) as session_end from INTERNAL_REPORTING_MV.CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY_INCOMPLETE
            ));
            output := OBJECT_CONSTRUCT('oldest_running', :oldest_running, 'newest_completed', :newest_completed, 'attempted_migrate', :migrate, 'migrate', :migrate1, 'migrate_INCOMPLETE', :migrate2,
                'new_records', 0, 'new_INCOMPLETE', 0, 'new_closed', 0, 'range_min', :range_min, 'range_max', :newest_completed);
        END IF;
        DROP TABLE RAW_WH_EVT;
        COMMIT;

    EXCEPTION
      WHEN OTHER THEN
        SYSTEM$LOG_ERROR(OBJECT_CONSTRUCT('error', 'Exception occurred while refreshing warehouse events.', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate));
        ROLLBACK;
        return OBJECT_CONSTRUCT('Error type', 'warehouse events error', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate);
    END;


    SYSTEM$LOG_INFO('Starting warehouse size table refresh');
    BEGIN
        BEGIN TRANSACTION;
            truncate INTERNAL.WAREHOUSE_SIZE_MAPPING;
            SHOW WAREHOUSES;
            insert into internal.warehouse_size_mapping select "name", "size", "type" from TABLE(RESULT_SCAN(LAST_QUERY_ID()));
        COMMIT;
    EXCEPTION
        WHEN OTHER THEN
            ROLLBACK;
            return OBJECT_CONSTRUCT('Error type', 'warehouse size mapping error', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate);
    END;

    SYSTEM$LOG_INFO('Finished warehouse size table refresh');
    CALL INTERNAL.SET_CONFIG('WAREHOUSE_EVENTS_MAINTENANCE', CURRENT_TIMESTAMP()::string);

    return output;
END;

CREATE OR REPLACE FUNCTION TOOLS.APPROX_CREDITS_USED(wh_name varchar, start_time timestamp, end_time timestamp)
RETURNS NUMBER
AS
$$
    TOOLS.WAREHOUSE_CREDITS_PER_MILLI(
        (select any_value(warehouse_size) from internal.warehouse_size_mapping t where t.warehouse_name = wh_name),
        coalesce((select any_value(warehouse_type) from internal.warehouse_size_mapping t where t.warehouse_name = wh_name), 'STANDARD'))*timestampdiff(millisecond, start_time, end_time)
$$;

CREATE OR REPLACE FUNCTION TOOLS.APPROX_CREDITS_USED(warehouse_name varchar, start_time timestamp)
RETURNS NUMBER
AS
$$
    tools.approx_credits_used(warehouse_name, start_time, current_timestamp())
$$;
