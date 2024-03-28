
CREATE TABLE INTERNAL.TASK_OPERATOR_STATS IF NOT EXISTS (run timestamp, success boolean, input variant, output variant);

CREATE TABLE INTERNAL_REPORTING.OPERATOR_STATS IF NOT EXISTS (
    QUERY_PARAMETERIZED_HASH VARCHAR(16777216),
    QUERY_ID VARCHAR(16777216),
    STEP_ID NUMBER(38,0),
    OPERATOR_ID NUMBER(38,0),
    PARENT_OPERATORS ARRAY,
    OPERATOR_TYPE VARCHAR(16777216),
    OPERATOR_STATISTICS VARIANT,
    EXECUTION_TIME_BREAKDOWN VARIANT,
    OPERATOR_ATTRIBUTES VARIANT);

CREATE TABLE INTERNAL_REPORTING.QUERY_PLANS IF NOT EXISTS (
    QUERY_PARAMETERIZED_HASH VARCHAR(16777216),
    QUERY_ID VARCHAR(16777216),
    PLAN_JSON VARCHAR(16777216)
);

create or replace procedure internal.get_plan(q varchar)
returns varchar
language sql
as
begin
let x varchar := (select system$explain_plan_json(:q));
return x;
exception
when other then
return null;
end;

create or replace procedure internal.fill_plan_stats()
returns string
language sql
as
begin
    let dt timestamp := current_timestamp();
    let input variant;
    begin
    begin transaction;
        input := (select output from INTERNAL.TASK_OPERATOR_STATS where success order by run desc limit 1);
        let oldest_running timestamp := 0::timestamp;

        if (input is not null) then
            oldest_running := input:oldest_running::timestamp;
        end if;

        let res resultset := (select query_id as q, query_parameterized_hash as h, end_time as e from account_usage.query_history where end_time > :oldest_running and start_time > dateadd(day, -14, current_timestamp()) and warehouse_size is not null order by end_time);
        let c1 cursor for res;
        let mt timestamp := 0::timestamp;
        let cnt number := 0;
        for record in c1 do
            let q varchar := record.q;
            let h varchar := record.h;
            let p varchar;
            call internal.get_plan(:q) into :p;
            let e timestamp := record.e;
            mt := (select greatest(:e, :mt));
            cnt := (select :cnt + 1);
            insert into internal_reporting.operator_stats select :h as query_parameterized_hash, * from table(get_query_operator_stats(:q));
            insert into internal_reporting.query_plans select :q as query_id, :h as query_parameterized_hash, :p as plan_json;
        end for;

    commit;
    insert into INTERNAL.TASK_OPERATOR_STATS SELECT :dt, true, :input, OBJECT_CONSTRUCT('oldest_running', :mt, 'new_records', :cnt)::VARIANT;
    EXCEPTION
      WHEN OTHER THEN
        SYSTEM$LOG_ERROR(OBJECT_CONSTRUCT('error', 'Exception occurred while refreshing operator stats.', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate));
        rollback;
        insert into INTERNAL.TASK_OPERATOR_STATS SELECT :dt, false, :input, OBJECT_CONSTRUCT('Error type', 'Other error', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate)::variant;
        RAISE;

    END;
end;
