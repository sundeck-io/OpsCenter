
-- sp to create view INTERNAL_REPORTING.QUERY_HISTORY_COMPLETE_AND_DAILY
CREATE OR REPLACE PROCEDURE INTERNAL.create_view_QUERY_HISTORY_COMPLETE_AND_DAILY()
    RETURNS STRING
    LANGUAGE SQL
AS
BEGIN
    execute immediate
    $$
        -- This view is a union of the complete and daily gridded queries.
        -- This includes daily gridded execution for queries still running but doesn't include complete queries for those queries still running.
        -- This data is intended to be serialized daily.
        CREATE OR REPLACE VIEW INTERNAL_REPORTING.QUERY_HISTORY_COMPLETE_AND_DAILY AS
        SELECT
            current_timestamp() as run_id,
            tools.qtag(query_text, true, true) as qtag,
            DATEDIFF('day', START_TIME, END_TIME) + 1 AS PERIOD_PLUS,
            -- Earlier versions generated COMPLETE and DAILY but accidentally applied the incorrect RECORD_TYPE to the given row.
            -- For new data, we generate COMPLETE_FIXED and DAILY_FIXED, and compensate in the views below.
            IFF(index = PERIOD_PLUS, 'COMPLETE_FIXED', 'DAILY_FIXED')::VARCHAR AS RECORD_TYPE,
            IFF(index in (0, PERIOD_PLUS), start_time, dateadd('day', index, date_trunc('day', start_time))) as ST,
            IFF(index in (PERIOD_PLUS - 1, PERIOD_PLUS), end_time, least(CURRENT_TIMESTAMP(), dateadd('day', index + 1, date_trunc('day', start_time)))) as ET,
            date_trunc('day', ST) AS ST_PERIOD,
            -- note that this formula can overcount compute consumption in the cases where snowflake reports
            -- peak load as opposed to average load. Ideally load percent would be area under the curve.
            DATEDIFF('milliseconds', ST, ET) * (0.01 * query_load_percent)* COALESCE(size.credits_per_milli, 0) AS unloaded_direct_compute_credits,
            DATEDIFF('milliseconds', ST, ET) AS DURATION,
            IFF(start_time > end_time, true, false) AS INCOMPLETE,
            START_TIME AS filterts,
            qh.*,
            tools.qtag_to_map(qtag) as qtag_filter

        FROM ACCOUNT_USAGE.QUERY_HISTORY AS qh
            LEFT OUTER JOIN INTERNAL_REPORTING.WAREHOUSE_CREDITS_PER_SIZE size ON qh.warehouse_size = size.warehouse_size,
            LATERAL FLATTEN(internal.period_range_plus('day', qh.start_time, qh.end_time)) emt(index)
        ;
    $$;
    RETURN 'Success';
END;

call INTERNAL.create_view_QUERY_HISTORY_COMPLETE_AND_DAILY();

create table internal_reporting_mv.query_history_complete_and_daily_incomplete if not exists  as select * from internal_reporting.query_history_complete_and_daily limit 0;
alter table internal_reporting_mv.query_history_complete_and_daily_incomplete add column if not exists qtag_filter variant;
create table internal_reporting_mv.query_history_complete_and_daily if not exists as select * from internal_reporting.query_history_complete_and_daily limit 0;
alter table internal_reporting_mv.query_history_complete_and_daily add column if not exists qtag_filter variant;

-- sp to create view reporting.enriched_query_history
CREATE OR REPLACE PROCEDURE INTERNAL.create_view_enriched_query_history()
    RETURNS STRING
    LANGUAGE SQL
AS
BEGIN
    execute immediate
    $$
        create or replace view reporting.enriched_query_history
        COPY GRANTS
        AS
            select
                qtag_filter,
                case warehouse_type when 'STANDARD' then 1.0 else 1.5 end * unloaded_direct_compute_credits * INTERNAL.GET_CREDIT_COST(warehouse_id) as COST,
                case warehouse_type when 'STANDARD' then 1.0 else 1.5 end * unloaded_direct_compute_credits as unloaded_direct_compute_credits,
                ST_PERIOD::DATE as ST_DAY,
                * exclude (period_plus, record_type, unloaded_direct_compute_credits, qtag_filter)
            -- We may have reversed RECORD_TYPE rows in the materialized table. Filter to the new "correct" RECORD_TYPE and the old "incorrect" RECORD_TYPE.
            from internal_reporting_mv.query_history_complete_and_daily where RECORD_TYPE in ('COMPLETE_FIXED', 'DAILY')
            union all
            select
                qtag_filter,
                case warehouse_type when 'STANDARD' then 1.0 else 1.5 end * unloaded_direct_compute_credits * INTERNAL.GET_CREDIT_COST(warehouse_id) as COST,
                case warehouse_type when 'STANDARD' then 1.0 else 1.5 end * unloaded_direct_compute_credits as unloaded_direct_compute_credits,
                ST_PERIOD::DATE as ST_DAY,
                * exclude (period_plus, record_type, unloaded_direct_compute_credits, qtag_filter)
            from internal_reporting_mv.query_history_complete_and_daily_incomplete where RECORD_TYPE in ('COMPLETE_FIXED', 'DAILY')
            ;
    $$;
    RETURN 'Success';
END;

call INTERNAL.create_view_enriched_query_history();

-- sp to create view reporting.enriched_query_history_daily
CREATE OR REPLACE PROCEDURE INTERNAL.create_view_enriched_query_history_daily()
    RETURNS STRING
    LANGUAGE SQL
AS
BEGIN
    execute immediate
    $$
    create or replace view reporting.enriched_query_history_daily
    COPY GRANTS
    as
        select
                qtag_filter,
        case warehouse_type when 'STANDARD' then 1.0 else 1.5 end * unloaded_direct_compute_credits * INTERNAL.GET_CREDIT_COST(warehouse_id) as COST,
                case warehouse_type when 'STANDARD' then 1.0 else 1.5 end * unloaded_direct_compute_credits as unloaded_direct_compute_credits,
                ST_PERIOD::DATE as ST_DAY,
            -- We may have reversed RECORD_TYPE rows in the materialized table. Filter to the new "correct" RECORD_TYPE and the old "incorrect" RECORD_TYPE.
            * exclude (period_plus, record_type, unloaded_direct_compute_credits, qtag_filter) from internal_reporting_mv.query_history_complete_and_daily where RECORD_TYPE in ('DAILY_FIXED', 'COMPLETE')
        union all
        select
                qtag_filter,
        case warehouse_type when 'STANDARD' then 1.0 else 1.5 end * unloaded_direct_compute_credits * INTERNAL.GET_CREDIT_COST(warehouse_id) as COST,
                case warehouse_type when 'STANDARD' then 1.0 else 1.5 end * unloaded_direct_compute_credits as unloaded_direct_compute_credits,
                ST_PERIOD::DATE as ST_DAY,
            * exclude (period_plus, record_type, unloaded_direct_compute_credits, qtag_filter) from internal_reporting_mv.query_history_complete_and_daily_incomplete where RECORD_TYPE in ('DAILY_FIXED', 'COMPLETE');
    $$;
    RETURN 'Success';
END;

call INTERNAL.create_view_enriched_query_history_daily();


-- sp to create view reporting.enriched_query_history_hourly
CREATE OR REPLACE PROCEDURE INTERNAL.create_view_enriched_query_history_hourly()
    RETURNS STRING
    LANGUAGE SQL
AS
BEGIN
    execute immediate
    $$
        -- Similar to above but subdivides queries by hour instead of day.
        -- It might be more efficient to expand from days to hours as opposed to here where we expand from complete to hours. We chose not
        -- to do this thinking this would be more efficient given less data read off disk (and the expansion typically collapses
        -- immediately with an aggregate).
        -- This view is not intended to be serialized.
        CREATE OR REPLACE VIEW reporting.enriched_query_history_hourly
        COPY GRANTS
        AS
        WITH QH AS (
            SELECT * EXCLUDE (ST, ET, unloaded_direct_compute_credits, cost, DURATION, ST_PERIOD)
            FROM reporting.enriched_query_history
        ),
        EXTRAS AS (
        SELECT
            DATEDIFF('hour', START_TIME, END_TIME) AS PERIOD_PLUS,
            IFF(index in (0, PERIOD_PLUS), start_time, dateadd('hour', index, date_trunc('hour', start_time))) as ST,
            IFF(index in (PERIOD_PLUS), end_time, least(CURRENT_TIMESTAMP(), dateadd('hour', index + 1, date_trunc('hour', start_time)))) as ET,
            -- inherit ST_DAY from QH as that will not change from a time truncation
            date_trunc('hour', ST) AS ST_PERIOD,
            -- note that this formula can overcount compute consumption in the cases where snowflake reports
            -- peak load as opposed to average load. Ideally load percent would be area under the curve.
            DATEDIFF('milliseconds', ST, ET) * (0.01 * query_load_percent)* COALESCE(size.credits_per_milli, 0) AS unloaded_direct_compute_credits,
            DATEDIFF('milliseconds', ST, ET) AS DURATION,
            unloaded_direct_compute_credits * INTERNAL.GET_CREDIT_COST(warehouse_id) as COST,
            qh.*
        FROM QH
        LEFT OUTER JOIN INTERNAL_REPORTING.WAREHOUSE_CREDITS_PER_SIZE size ON qh.warehouse_size = size.warehouse_size,
        LATERAL FLATTEN(internal.period_range('hour', qh.start_time, qh.end_time)) emt(index)
        )
        SELECT * EXCLUDE  (PERIOD_PLUS) FROM EXTRAS;
        ;
    $$;
    RETURN 'Success';
END;

call INTERNAL.create_view_enriched_query_history_hourly();

CREATE OR REPLACE VIEW REPORTING.LABELED_QUERY_HISTORY AS SELECT * FROM REPORTING.enriched_query_history;

-- Cluster the table for complete Query History rows by record_type/st_period. We want this to be done exactly once.
DECLARE
    key text default 'CLUSTERED_QUERY_HISTORY';
BEGIN
    let already_clustered text;
    call internal.get_config(:key) into :already_clustered;
    if (already_clustered is null OR already_clustered <> 'true') then
        SYSTEM$LOG_INFO('Clustering QUERY_HISTORY_COMPLETE_AND_DAILY by (RECORD_TYPE, ST_PERIOD::DATE)');
        ALTER TABLE INTERNAL_REPORTING_MV.QUERY_HISTORY_COMPLETE_AND_DAILY CLUSTER BY (RECORD_TYPE, ST_PERIOD::date);
        call internal.set_config(:key, 'true');
    end if;
END;

create or replace procedure internal.update_qtag_day()
returns number
language sql
comment = 'materialize qtag_filter for a single day until fully backfilled'
as
begin
UPDATE internal_reporting_mv.query_history_complete_and_daily
SET    qtag_filter=tools.qtag_to_map(qtag)
WHERE  start_time::date =
       (
              SELECT max(start_time::date)
              FROM   internal_reporting_mv.query_history_complete_and_daily
              WHERE  qtag_filter IS NULL
              AND    qtag IS NOT NULL)
AND    qtag_filter IS NULL
AND    qtag IS NOT NULL;

let updates number :=
(
       SELECT $1
       FROM   TABLE(result_scan(last_query_id())));


UPDATE internal_reporting_mv.query_history_complete_and_daily_incomplete
SET    qtag_filter = tools.qtag_to_map(qtag)
WHERE  start_time :: DATE = (SELECT Max(start_time :: DATE)
                             FROM
              internal_reporting_mv.query_history_complete_and_daily_incomplete
                             WHERE  qtag_filter IS NULL
                                    AND qtag IS NOT NULL)
       AND qtag_filter IS NULL
       AND qtag IS NOT NULL;

updates := (select $1 from table(result_scan(last_query_id()))) + :updates;

return updates;
exception
    when other then
        SYSTEM$LOG_ERROR(OBJECT_CONSTRUCT('error', 'Exception occurred while updating qtag filter column.', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate));
end;
