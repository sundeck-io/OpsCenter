
CREATE OR REPLACE VIEW INTERNAL_REPORTING.CLUSTER_AND_WAREHOUSE_SESSIONS AS

WITH

-- This query names the key events in session history and fiters down to those that we care about.
EVENTS AS (
    SELECT
        timestamp as filterts,
        timestamp,
        warehouse_id,
        warehouse_name,
        cluster_number,
        case
            when event_name in ('RESUME_WAREHOUSE','SUSPEND_WAREHOUSE') then 'WH'
            when event_name in ('RESUME_CLUSTER','SUSPEND_CLUSTER') then 'CL'
        END as SESSION_TYPE,
        case
            when event_name = 'RESUME_WAREHOUSE' and event_state = 'STARTED' then 1
            when event_name = 'RESUME_CLUSTER' and event_state = 'COMPLETED' then 1
            when event_name = 'SUSPEND_CLUSTER' and event_state = 'COMPLETED' then 2
            when event_name = 'SUSPEND_WAREHOUSE' and event_state = 'STARTED' then 2
            else null
        end as SESSION_BEHAVIOR,
        case
            when event_name = 'RESUME_WAREHOUSE' and event_state = 'STARTED' then 1
            when event_name = 'RESUME_CLUSTER' and event_state = 'COMPLETED' then 2
            when event_name = 'SUSPEND_CLUSTER' and event_state = 'COMPLETED' then 3
            when event_name = 'SUSPEND_WAREHOUSE' and event_state = 'STARTED' then 4
            else null
        end as EVENT
    FROM ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY
    WHERE (event_name, event_state) IN (
        ('RESUME_WAREHOUSE', 'STARTED'),
        ('SUSPEND_WAREHOUSE', 'STARTED'),
        ('RESUME_CLUSTER', 'COMPLETED'),
        ('SUSPEND_CLUSTER', 'COMPLETED')
        )
        AND EVENT IS NOT NULL
),


-- Create warehouse sessions and associate cluster sessions with each warehouse session.
-- TODO: consider skipping this step. It isn't clear how we're going to use the cluster => session_id relationship
WSSES AS (
SELECT * FROM
EVENTS
MATCH_RECOGNIZE (
  PARTITION BY warehouse_id
  ORDER BY timestamp, event -- order by event here just in case cluster stop and warehouse stop have same timestamp
  MEASURES
    BITOR(
        BITSHIFTLEFT(warehouse_id, 64),
        DATEDIFF('millisecond', '1970-01-01', FIRST(timestamp))) as SESSION_ID
  ALL ROWS PER MATCH
  PATTERN (STARTING? OTHER* STOPPED?)
  DEFINE
    STARTING as event = 1,
    OTHER AS event IN (2,3),
    STOPPED AS event = 4
    )
),

-- Create a record per each warehouse and cluster session. We do this in parallel. (These
-- matches are partitioned differently than the warehouse match recognize above.)
COLLAPSE_SESSIONS AS (
    SELECT *
    FROM WSSES
    MATCH_RECOGNIZE (
      PARTITION BY SESSION_TYPE, session_id, warehouse_id, warehouse_name, cluster_number
      ORDER BY timestamp
      MEASURES
        FIRST(filterts) AS filterts,
        FIRST(timestamp) AS SESSION_START,
        IFF((FINAL LAST(event)) in (3,4), timestamp, least(current_timestamp(), date_trunc('day', timestamp)+ interval '1 days')) AS SESSION_END,
        IFF((FINAL LAST(event)) in (3,4), FALSE, IFF(current_timestamp() < date_trunc('day', timestamp)+ interval '1 days', TRUE, FALSE)) AS INCOMPLETE
      ONE ROW PER MATCH
      PATTERN (STARTING STOPPED?)
      DEFINE
        STARTING as SESSION_BEHAVIOR = 1,
        STOPPED AS SESSION_BEHAVIOR = 2
      )
)
SELECT
    *,
    DATEDIFF('milliseconds', SESSION_START, SESSION_END) AS DURATION_MS
FROM COLLAPSE_SESSIONS
;

CREATE OR REPLACE VIEW INTERNAL_REPORTING.CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY
AS
SELECT
    current_timestamp() as run_id,
    filterts,
    warehouse_id,
    WAREHOUSE_NAME,
    CLUSTER_NUMBER,
    SESSION_TYPE,
    SESSION_ID,
    session_start,
    session_end,
    DATEDIFF('day', session_start, session_end) + 1 AS PERIOD_PLUS,
    IFF(index = PERIOD_PLUS, 'COMPLETE_FIXED', 'DAILY_FIXED')::VARCHAR AS RECORD_TYPE,
    IFF(index in (0, PERIOD_PLUS), session_start, dateadd('day', index, date_trunc('day', session_start))) as ST,
    IFF(index in (PERIOD_PLUS - 1, PERIOD_PLUS), session_end, least(CURRENT_TIMESTAMP(), dateadd('day', index + 1, date_trunc('day', session_end)))) as ET,
    date_trunc('day', ST) AS ST_PERIOD,
    DATEDIFF('milliseconds', ST, ET) AS DURATION,
    INCOMPLETE
FROM INTERNAL_REPORTING.CLUSTER_AND_WAREHOUSE_SESSIONS evts,
LATERAL FLATTEN(internal.period_range_plus('day', evts.session_start, evts.session_end)) emt(index)
;

create table INTERNAL_REPORTING_MV.CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY IF NOT EXISTS AS SELECT * FROM INTERNAL_REPORTING.CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY LIMIT 0;
create table INTERNAL_REPORTING_MV.CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY_INCOMPLETE IF NOT EXISTS AS SELECT * FROM INTERNAL_REPORTING.CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY LIMIT 0;


create or replace view reporting.warehouse_sessions as
    select * exclude (period_plus, record_type, session_type) from internal_reporting_mv.CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY where RECORD_TYPE in ('COMPLETE_FIXED', 'DAILY') AND session_type = 'WH'
    union all
    select * exclude (period_plus, record_type, session_type) from internal_reporting_mv.CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY_INCOMPLETE where RECORD_TYPE in ('COMPLETE_FIXED', 'DAILY') AND session_type = 'WH'
    ;


create or replace view reporting.cluster_sessions as
    select * exclude (period_plus, record_type, session_type) from internal_reporting_mv.CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY where RECORD_TYPE in ('COMPLETE_FIXED', 'DAILY') AND session_type = 'CL'
    union all
    select * exclude (period_plus, record_type, session_type) from internal_reporting_mv.CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY_INCOMPLETE where RECORD_TYPE in ('COMPLETE_FIXED', 'DAILY') AND session_type = 'CL'
    ;

create or replace view reporting.warehouse_sessions_daily as
    select * exclude (period_plus, record_type, session_type) from internal_reporting_mv.CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY where RECORD_TYPE in ('DAILY_FIXED', 'COMPLETE')
    union all
    select * exclude (period_plus, record_type, session_type) from internal_reporting_mv.CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY_INCOMPLETE where RECORD_TYPE in ('DAILY_FIXED', 'COMPLETE');

create or replace view reporting.cluster_sessions_daily as
    select * exclude (period_plus, record_type, session_type) from internal_reporting_mv.CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY where RECORD_TYPE in ('DAILY_FIXED', 'COMPLETE') AND session_type = 'CL'
    union all
    select * exclude (period_plus, record_type, session_type) from internal_reporting_mv.CLUSTER_AND_WAREHOUSE_SESSIONS_COMPLETE_AND_DAILY_INCOMPLETE where RECORD_TYPE in ('DAILY_FIXED', 'COMPLETE') AND session_type = 'CL';


CREATE OR REPLACE VIEW reporting.warehouse_sessions_hourly AS
SELECT
    SESSION_ID,
    warehouse_id,
    IFF(index = 0, st, dateadd('hour', index, date_trunc('hour', st))) as st,
    IFF(index = DATEDIFF('hour', st, et), et, dateadd('hour', index + 1, date_trunc('hour', st))) as et,
    DATEDIFF('milliseconds', st, et) AS duration
FROM reporting.warehouse_sessions evts,
LATERAL FLATTEN(internal.period_range('hour', evts.st, evts.et)) emt(index)
;

CREATE OR REPLACE VIEW reporting.cluster_sessions_hourly AS
SELECT
    SESSION_ID,
    warehouse_id,
    cluster_number,
    IFF(index = 0, st, dateadd('hour', index, date_trunc('hour', st))) as st,
    IFF(index = DATEDIFF('hour', st, et), et, dateadd('hour', index + 1, date_trunc('hour', st))) as et,
    DATEDIFF('milliseconds', st, et) AS DURATION_HOURLY_MS
FROM reporting.warehouse_sessions evts,
LATERAL FLATTEN(internal.period_range('hour', evts.st, evts.et)) emt(index)
;
