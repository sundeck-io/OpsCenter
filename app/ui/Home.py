import streamlit as st
from modules import add_custom_modules
import setup
import sthelp
from connection import execute_with_cache
import datetime

add_custom_modules()


st.set_page_config(layout="wide", page_title="Sundeck Opscenter", page_icon=":pilot:")

setup.setup_permissions()


def timedelta_to_human_readable(delta: datetime.timedelta) -> str:
    if delta.total_seconds() < 0:
        return None
    # Break down the timedelta into days, hours, minutes, and seconds
    days, seconds = delta.days, delta.seconds
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60

    # Build a human-readable string
    parts = []
    if days > 0:
        parts.append(f"{days} day{'s' if days > 1 else ''}")
    if hours > 0:
        parts.append(f"{hours} hour{'s' if hours > 1 else ''}")
    if minutes > 0:
        parts.append(f"{minutes} minute{'s' if minutes > 1 else ''}")
    if seconds > 0 or not parts:
        parts.append(f"{seconds} second{'s' if seconds != 1 else ''}")

    return ", ".join(parts)


def get(query, fmt):
    df = execute_with_cache(query)
    if df is None:
        return fmt(None)
    if df.empty:
        return fmt(None)
    util = df.iloc[0, 0]
    if util is None:
        return fmt(None)
    return fmt(util)


def to_str(end_time, start_time):
    """
    Convert end and start times to a human readable string
    showing the last run time and the time the run took.
    Also returns the next time the task will run.
    """
    if not end_time:
        return None, None
    if not start_time:
        return None, None

    update_str = end_time.strftime("%Y-%m-%d %H:%M")
    duration = timedelta_to_human_readable(end_time - start_time)
    next_time_str = "60 minutes"

    if duration is not None:
        return f"{update_str} ({duration})", next_time_str
    return update_str, next_time_str


def get_refresh_data():
    """
    We get the following data in this function:
    * q1 = Query History Last Update. Both duration and time taken.
    * q2 = Query History Update Frequency.
    * w1 = Warehouse Events Last Update. Both duration and time taken.
    * w2 = Warehouse Events Update Frequency.
    * rmin = Minimum date in the enriched query history table.
    * rmax = Maximum date in the enriched query history table.
    """
    wh_end_times = (
        "select value from catalog.config where key = 'WAREHOUSE_EVENTS_MAINTENANCE'"
    )
    query_end_times = (
        "select value from catalog.config where key  = 'QUERY_HISTORY_MAINTENANCE'"
    )
    queryh_start_times = "select max(task_start) from internal.task_log where success AND object_type = 'QUERY_HISTORY' AND object_name = 'QUERY_HISTORY'"
    warehouse_start_times = "select max(task_start) from internal.task_log where success AND object_type = 'WAREHOUSE_EVENTS' AND object_name = 'WAREHOUSE_EVENTS_HISTORY'"
    range_min = "select min(start_time) from reporting.enriched_query_history"
    range_max = "select max(start_time) from reporting.enriched_query_history"
    qet = get(
        query_end_times,
        lambda x: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f %z")
        if x is not None
        else None,
    )
    wet = get(
        wh_end_times,
        lambda x: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f %z")
        if x is not None
        else None,
    )
    qst = get(
        queryh_start_times,
        lambda x: x.replace(tzinfo=qet.tzinfo)
        if x is not None and qet is not None
        else None,
    )
    wst = get(
        warehouse_start_times,
        lambda x: x.replace(tzinfo=wet.tzinfo)
        if x is not None and wet is not None
        else None,
    )
    q1, q2 = to_str(qet, qst)
    w1, w2 = to_str(wet, wst)
    rmin = get(
        range_min, lambda x: x.strftime("%Y-%m-%d %H:%M") if x is not None else None
    )
    rmax = get(
        range_max, lambda x: x.strftime("%Y-%m-%d %H:%M") if x is not None else None
    )
    return q1, q2, w1, w2, rmin, rmax


def get_wh_utilization():
    query = """select iff(sum(loaded_cc) = 0,null, sum(unloaded_cc)/sum(loaded_cc)) as utilization
        from reporting.warehouse_daily_utilization
        where period::DATE between current_timestamp - interval '30 days' and current_timestamp
        and not internal.is_serverless_warehouse(warehouse_name)"""
    return get(query, lambda x: f"{x*100:.2f} %" if x is not None else "Calculating")


sthelp.image_svg("opscenter_logo.svg")
cols = st.columns([30, 20])
with cols[0]:
    st.markdown(
        """
    The Sundeck native app is part of the Sundeck query engineering platform.
    The native app is designed to give you better insight into your Snowflake
    consumption and provide a useful set of tools to improve behavior.
    """
    )

    wh = get_wh_utilization()
    st.metric("Your current aggregate warehouse utilization.", wh)
    if wh == "Calculating":
        st.markdown(
            """
Note: Upon installation, the Sundeck native app analyzes your Snowflake usage
history. Depending on the size of this data, this may takes minutes to hours.
Once complete, utilization will reported above.
        """
        )
    st.markdown(
        """

## Snowflake Management Tools

Sundeck includes several entirely native tools for better managing Snowflake. These
can be used entirely from SQL or with the [Sundeck UI](https://sundeck.io/try)
(please open this link by right clicking and selecting open in new tab).

### Monitors
Automatically monitor currently running queries on Snowflake and alert when certain
patterns or behaviors are detected. Alert via Slack or Email. Additionally can be
configured to automatically cancel undesireable query patterns. Additional details can be found [here](https://docs.sundeck.io/concepts/query-monitors/).
### Warehouse Schedules
Set schedules to automatically adapt warehouse settings over time. For example, size
down warehouses on evenings and weekends to reduce waste. Additional details can be found [here](https://docs.sundeck.io/concepts/warehouse-management/).
### Labels
Labels allow you to categorize queries based on query shape and consumption. Labels
can facilitate sub-warehouse activity tracking and analysis. Additional details can be found [here](https://docs.sundeck.io/concepts/labels/).

## Enhanced Snowflake Consumption Tables
The Sundeck native app automatically generates a fully materialized and incrementally
updated set of warehouse and query activity data tables. This allows you to analyze
key `SNOWFLAKE.ACCOUNT_USAGE` tables efficiently. These tables also are enriched to
enhance analysis opportunities. Key tables include:

#### REPORTING.ENRICHED_QUERY_HISTORY
An enriched variation of `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`. Includes all
columns from that tables as well as per query estimated costs using Sundeck's cost
attribution model and automatically extracted QTag values.

#### REPORTING.WAREHOUSE_SESSIONS
A sessionized version of `WAREHOUSE_EVENT_HISTORY` focused on warehouse start and
stop events. Each record in this table describes a warehouse from start to suspend.

#### REPORTING.CLUSTER_SESSIONS
A sessionized version of `WAREHOUSE_EVENT_HISTORY` focused on cluster activity.
Each record in this table describes one cluster start and stop. Clusters are the
individual auto-scaling units within a warehouse. If you are not using autoscaling,
this table will be similar to warehouse sessions above.

#### REPORTING.WAREHOUSE_DAILY_UTILIZATION and WAREHOUSE_HOURLY_UTILIZATION
Provides daily and hourly versions of per warehouse utilization analyses. Useful to
understand where warehouse efficiency can be improved.
"""
    )
with cols[1]:
    qlu, qnu, wlu, wnu, das, dae = get_refresh_data()
    with st.expander("", expanded=True):
        if qlu is None:
            st.markdown(
                """
                ### Current Data Status

                Initial data load is processing. This may take up to one hour.
                You can monitor the task process by viewing 'TASK NAME' in
                Monitoring > Task History within Snowsight"""
            )
        else:
            st.markdown("### Current Data Status")
            st.write(f"Data Available: {das} - {dae}")
            st.write(f"Query History Last Update: {qlu}")
            st.write(f"Query History Update Frequency: {qnu}")
            st.write(f"Warehouse Events Last Update: {wlu}")
            st.write(f"Warehouse Events History Update Frequency: {wnu}")

    with st.expander("", expanded=True):
        st.markdown(
            """
            ### Connect to Sundeck

            The native app is better when connected to the Sundeck UI. This is
            free and lets you manage query monitors, warehouse schedules and labels
            as well as take advantage of further features via a comprehensive UI."""
        )
        st.markdown("[Connect to Sundeck UI](https://sundeck.io/signup)")
        st.video("https://youtu.be/msenvc42pYo")
