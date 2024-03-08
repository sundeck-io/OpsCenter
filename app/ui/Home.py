import streamlit as st
import sthelp


# sthelp.chrome()
st.set_page_config(layout="wide", page_title="Sundeck Opscenter", page_icon=":pilot:")


def get_refresh_data():
    return "2021-01-01", "2021-01-31", "2021-01-01", "2021-02-01"


def get_wh_utilization():
    return "20 %"  # This should be a real value


cols = st.columns([1, 30, 20, 1])
with cols[1]:
    sthelp.image_svg("opscenter_logo.svg")
    st.markdown(
        """
    The Sundeck native app is part of the Sundeck query engineering platform.
    The native app is designed to give you better insight into your Snowflake
    consumption and provide a useful set of tools to improve behavior.
    """
    )

    wh = get_wh_utilization()
    st.metric("Warehouse Utilization", wh)
    if wh == "Calculating":
        st.markdown(
            """
Note: Upon installation, the Sundeck native app analyzes your Snowflake usage
history. Depending on the size of this data, this may takes minutes to hours.
Once complete, utilization will reported above. (this is only shown until
first materialization
        """
        )
    st.markdown(
        """

## Enhanced Snowflake Consumption Tables
The Sundeck native app automatically generates a fully materialized and incrementally
updated set of warehouse and query activity data tables. This allows you to analyze
key `SNOWFLAKE.ACCOUNT_USAGE` tables efficiently. These tables also are enriched to
enhance analysis opportunities. Key tables include:

#### REPORTING.ENRICHED_QUERY_HISTORY
An enriched variation of `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`. Includes all
columns from that tables as well as per query estimated costs using Sundeck's cost
attribution model and automatically extracted QTag values. Additional details can be
found here.

#### REPORTING.WAREHOUSE_SESSIONS
A sessionized version of `WAREHOUSE_EVENT_HISTORY` focused on warehouse start and
stop events. Each record in this table describes a warehouse from start to suspend.
Additional details can be found here.

#### REPORTING.CLUSTER_SESSIONS
A sessionized version of `WAREHOUSE_EVENT_HISTORY` focused on cluster activity.
Each record in this table describes one cluster start and stop. Clusters are the
individual auto-scaling units within a warehouse. If you are not using autoscaling,
this table will be similar to warehouse sessions above. Additional details can be
found here.

#### REPORTING.WAREHOUSE_DAILY_UTILIZATION and WAREHOUSE_HOURLY_UTILIZATION
Provides daily and hourly versions of per warehouse utilization analyses. Useful to
understand where warehouse efficiency can be improved. Additional details can be found here.

## Snowflake Management Tools

Sundeck includes several entirely native tools for better managing Snowflake. These
can be used entirely from SQL or with the Sundeck UI (send to landing page).
### Monitors
Automatically monitor currently running queries on Snowflake and alert when certain
patterns or behaviors are detected. Alert via Slack or Email. Additionally can be
configured to automatically cancel undesireable query patterns. Additional details can be found here.
### Warehouse Schedules
Set schedules to automatically adapt warehouse settings over time. For example, size
down warehouses on evening and weekends to reduce waste. Additional details can be found here.
### Labels
Labels allow you to categorize queries based on query shape and consumption. Labels
can facilitate sub-warehouse activity tracking and analysis. Additional details can be found here.

                """
    )
with cols[2]:
    start, end, last, next = get_refresh_data()
    with st.expander("", expanded=True):
        if start is None:
            st.markdown(
                """Initial data load is processing. This may take up to one hour.
                You can monitor the task process by viewing 'TASK NAME' in
                Monitoring > Task History within Snowsight"""
            )
        else:
            st.write(f"Data Available: {start} - {end}")
            st.write(f"Last Update: {last}")
            st.write(f"Next Update: {next}")

    with st.expander("", expanded=True):
        st.markdown(
            """The native app is better when connecting to the Sundeck UI. This is
            free and lets you manage query monitors, warehouse schedules and labels
            as well as take advantage of further features via a comprehensive UI."""
        )
        if st.button("Connect to Sundeck UI"):
            pass
