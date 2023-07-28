
-- This view is a union of the complete and daily gridded queries.
-- This includes daily gridded execution for queries still running but doesn't include complete queries for those queries still running.
-- This data is intended to be serialized daily.
CREATE OR REPLACE VIEW INTERNAL_REPORTING.QUERY_HISTORY_COMPLETE_AND_DAILY AS
SELECT
    current_timestamp() as run_id,
    tools.qtag(qh.query_text, true, true) as qtag,
    DATEDIFF('day', START_TIME, END_TIME) + 1 AS PERIOD_PLUS,
    IFF(index = PERIOD_PLUS, 'DAILY', 'COMPLETE') AS RECORD_TYPE,
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
    sessions.created_on,
    sessions.authentication_method,
    sessions.login_event_id,
    sessions.client_application_version,
    sessions.client_application_id,
    sessions.client_environment,
    sessions.client_build_id,
    sessions.client_version,
    sessions.closed_reason,
    tasks.name as task_name,
    tasks.condition_text as task_condition_text,
    tasks.schema_name as task_schema_name,
    tasks.task_schema_id,
    tasks.database_name as task_database_name,
    tasks.task_database_id,
    tasks.scheduled_time as task_scheduled_time,
    tasks.completed_time as task_completed_time,
    tasks.state as task_state,
    tasks.return_value as task_return_value,
    tasks.error_code as task_error_code,
    tasks.error_message as task_error_message,
    tasks.graph_version as task_graph_version,
    tasks.run_id as task_run_id,
    tasks.root_task_id,
    tasks.scheduled_from as task_scheduled_from
FROM ACCOUNT_USAGE.QUERY_HISTORY AS qh
    LEFT OUTER JOIN INTERNAL_REPORTING.WAREHOUSE_CREDITS_PER_SIZE size ON qh.warehouse_size = size.warehouse_size
    LEFT OUTER JOIN ACCOUNT_USAGE.SESSIONS sessions ON qh.session_id = sessions.session_id
    LEFT OUTER JOIN ACCOUNT_USAGE.TASK_HISTORY tasks ON qh.query_id = tasks.query_id,
    LATERAL FLATTEN(internal.period_range_plus('day', qh.start_time, qh.end_time)) emt(index)
;
create table internal_reporting_mv.query_history_complete_and_daily_incomplete if not exists  as select * from internal_reporting.query_history_complete_and_daily limit 0;
create table internal_reporting_mv.query_history_complete_and_daily if not exists as select * from internal_reporting.query_history_complete_and_daily limit 0;

create or replace view reporting.enriched_query_history as
    select tools.qtag_to_map(qtag) as qtag_filter, unloaded_direct_compute_credits * INTERNAL.GET_CREDIT_COST(warehouse_id) as COST, * exclude (period_plus, record_type) from internal_reporting_mv.query_history_complete_and_daily where RECORD_TYPE = 'COMPLETE'

    union all
    select tools.qtag_to_map(qtag) as qtag_filter, unloaded_direct_compute_credits * INTERNAL.GET_CREDIT_COST(warehouse_id) as COST, * exclude (period_plus, record_type) from internal_reporting_mv.query_history_complete_and_daily_incomplete where RECORD_TYPE = 'COMPLETE'
    ;

create or replace view reporting.enriched_query_history_daily as
    select tools.qtag_to_map(qtag) as qtag_filter, unloaded_direct_compute_credits * INTERNAL.GET_CREDIT_COST(warehouse_id) as COST, * exclude (period_plus, record_type) from internal_reporting_mv.query_history_complete_and_daily where RECORD_TYPE = 'DAILY'
    union all
    select tools.qtag_to_map(qtag) as qtag_filter, unloaded_direct_compute_credits * INTERNAL.GET_CREDIT_COST(warehouse_id) as COST, * exclude (period_plus, record_type) from internal_reporting_mv.query_history_complete_and_daily_incomplete where RECORD_TYPE = 'DAILY';

-- Similar to above but subdivides queries by hour instead of day.
-- It might be more efficient to expand from days to hours as opposed to here where we expand from complete to hours. We chose not
-- to do this thinking this would be more efficient given less data read off disk (and the expansion typically collapses
-- immediately with an aggregate).
-- This view is not intended to be serialized.
CREATE OR REPLACE VIEW reporting.enriched_query_history_hourly AS
WITH QH AS (
    SELECT * EXCLUDE (ST, ET, ST_PERIOD, unloaded_direct_compute_credits, cost, DURATION)
    FROM reporting.enriched_query_history
),
EXTRAS AS (
SELECT
    DATEDIFF('hour', START_TIME, END_TIME) AS PERIOD_PLUS,
    IFF(index in (0, PERIOD_PLUS), start_time, dateadd('hour', index, date_trunc('hour', start_time))) as ST,
    IFF(index in (PERIOD_PLUS), end_time, least(CURRENT_TIMESTAMP(), dateadd('hour', index + 1, date_trunc('hour', start_time)))) as ET,
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

CREATE OR REPLACE VIEW REPORTING.LABELED_QUERY_HISTORY AS SELECT * FROM REPORTING.enriched_query_history;
