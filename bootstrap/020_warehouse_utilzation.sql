
CREATE OR REPLACE VIEW REPORTING.WAREHOUSE_DAILY_UTILIZATION AS
with QUERY_WH_UTIL AS (
select
    ST_PERIOD,
    case when internal.is_serverless_warehouse(WAREHOUSE_NAME) then 'Serverless Task' else WAREHOUSE_NAME end as WAREHOUSE_NAME,
    case when internal.is_serverless_warehouse(WAREHOUSE_NAME) then -1 else WAREHOUSE_ID end as WAREHOUSE_ID,
    COUNT(*) AS QUERIES_EXECUTED,
    SUM(unloaded_direct_compute_credits) AS UNLOADED_COMPUTE_CREDITS
from REPORTING.ENRICHED_QUERY_HISTORY_DAILY
where END_TIME < timestampadd(minute, -180, current_timestamp)
AND NOT internal.is_serverless_warehouse(WAREHOUSE_NAME)
GROUP BY 1,2,3
),
WAREHOUSE_PERIODIC AS (
select DATE_TRUNC('day', START_TIME) AS M_PERIOD, WAREHOUSE_ID, SUM(CREDITS_USED_COMPUTE) AS LOADED_COMPUTE_CREDITS, SUM(CREDITS_USED_CLOUD_SERVICES) AS WAREHOUSE_CLOUD_CREDITS, SUM(CREDITS_USED_COMPUTE) + SUM(CREDITS_USED_CLOUD_SERVICES) AS TOTAL_CREDITS, TOTAL_CREDITS * INTERNAL.GET_CREDIT_COST() AS COST
FROM ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
GROUP BY M_PERIOD, WAREHOUSE_ID, WAREHOUSE_NAME
--UNION ALL
--select date_trunc('day', start_time), -1, sum(credits_used), 0, sum(credits_used), sum(credits_used) * INTERNAL.GET_SERVERLESS_CREDIT_COST() from account_usage.serverless_task_history group by 1
)
select
    COALESCE(ST_PERIOD, M_PERIOD) AS PERIOD,
    COALESCE(QUERY_WH_UTIL.WAREHOUSE_ID, WAREHOUSE_PERIODIC.WAREHOUSE_ID) AS WAREHOUSE_ID,
    COALESCE(QUERY_WH_UTIL.WAREHOUSE_NAME, 'Unknown') AS WAREHOUSE_NAME,
    COALESCE(QUERIES_EXECUTED, 0) AS QUERIES,
    CASE
        WHEN UNLOADED_COMPUTE_CREDITS IS NULL THEN 0
        -- use this to correct for unloaded credit overaccounting
        WHEN UNLOADED_COMPUTE_CREDITS > LOADED_COMPUTE_CREDITS THEN LOADED_COMPUTE_CREDITS
        ELSE UNLOADED_COMPUTE_CREDITS
    END AS UNLOADED_CC,
    COALESCE(LOADED_COMPUTE_CREDITS, 0) AS LOADED_CC,
    IFF(LOADED_CC = 0,null, UNLOADED_CC/LOADED_CC) AS UTILIZATION
FROM
    QUERY_WH_UTIL
    FULL OUTER JOIN WAREHOUSE_PERIODIC ON QUERY_WH_UTIL.WAREHOUSE_ID = WAREHOUSE_PERIODIC.WAREHOUSE_ID AND ST_PERIOD = M_PERIOD;
;

-- NOTE that this query does not include serverless tasks
CREATE OR REPLACE VIEW REPORTING.WAREHOUSE_HOURLY_UTILIZATION AS
with QUERY_WH_UTIL AS (
select
    ST_PERIOD,
    WAREHOUSE_NAME,
    WAREHOUSE_ID,
    COUNT(*) AS QUERIES_EXECUTED,
    SUM(unloaded_direct_compute_credits) AS UNLOADED_COMPUTE_CREDITS
from REPORTING.ENRICHED_QUERY_HISTORY_HOURLY
WHERE NOT INTERNAL.IS_SERVERLESS_WAREHOUSE(WAREHOUSE_NAME)
GROUP BY ST_PERIOD, WAREHOUSE_ID, WAREHOUSE_NAME
),
WAREHOUSE_PERIODIC AS (
select DATE_TRUNC('hour', START_TIME) AS M_PERIOD, WAREHOUSE_ID, SUM(CREDITS_USED_COMPUTE) AS LOADED_COMPUTE_CREDITS, SUM(CREDITS_USED_CLOUD_SERVICES) WARESHOUSE_CLOUD_CREDITS
FROM ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
GROUP BY M_PERIOD, WAREHOUSE_ID
)
select
    COALESCE(ST_PERIOD, M_PERIOD) AS PERIOD,
    COALESCE(QUERY_WH_UTIL.WAREHOUSE_ID, WAREHOUSE_PERIODIC.WAREHOUSE_ID) AS WAREHOUSE_ID,
    COALESCE(QUERY_WH_UTIL.WAREHOUSE_NAME, 'Unknown') AS WAREHOUSE_NAME,
    COALESCE(QUERIES_EXECUTED, 0) AS QUERIES,
    CASE
        WHEN UNLOADED_COMPUTE_CREDITS IS NULL THEN 0
        -- use this to correct for unloaded credit overaccounting
        WHEN UNLOADED_COMPUTE_CREDITS > LOADED_COMPUTE_CREDITS THEN LOADED_COMPUTE_CREDITS
        ELSE UNLOADED_COMPUTE_CREDITS
    END AS UNLOADED_CC,
    COALESCE(LOADED_COMPUTE_CREDITS, 0) AS LOADED_CC,
    IFF(LOADED_CC = 0,null, UNLOADED_CC/LOADED_CC) AS UTILIZATION
FROM
    QUERY_WH_UTIL
    FULL OUTER JOIN WAREHOUSE_PERIODIC ON QUERY_WH_UTIL.WAREHOUSE_ID = WAREHOUSE_PERIODIC.WAREHOUSE_ID AND ST_PERIOD = M_PERIOD;
;

create or replace function reporting.WAREHOUSE_UTILIZATION_HEATMAP(INPUT_START_DATE timestamp, INPUT_END_DATE timestamp, INPUT_WAREHOUSE_NAME string)
returns table (period timestamp, utilization number(38,16))
as $$
	with ccost as (
	  select coalesce(value, 2.0) as value from internal.config where key='compute_credit_cost'),
    util as (
        select date_trunc('day', period) as period, sum(loaded_cc * (select any_value(value) from ccost)) as cost, iff(sum(loaded_cc) = 0,null, sum(unloaded_cc)/sum(loaded_cc)) as utilization
        from reporting.warehouse_daily_utilization
        where period between INPUT_START_DATE and INPUT_END_DATE and case when INPUT_WAREHOUSE_NAME is null then true else warehouse_name = INPUT_WAREHOUSE_NAME end
        group by 1
	)

    select cast(d.date as timestamp) as period, utilization from
    internal.dates d
    left outer join util u on d.date = u.period
    where d.date between INPUT_START_DATE and INPUT_END_DATE
$$;
