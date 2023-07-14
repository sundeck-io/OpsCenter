import plotly.graph_objects as go
import streamlit as st
import filters
from telemetry import page_view

import connection


def report(
    bf: filters.BaseFilter,
    cost_per_credit,
):
    def warehouse_stats(container):

        sql = f"""
    select date_trunc('{bf.trunc()}', PERIOD) AS DT, SUM(LOADED_CC * {cost_per_credit}) AS COST, IFF(SUM(LOADED_CC) = 0,null, SUM(UNLOADED_CC)/SUM(LOADED_CC)) AS UTILIZATION
    FROM REPORTING.WAREHOUSE_{bf.tbl()}_UTILIZATION
    where PERIOD between %(start)s and %(end)s and (array_size(%(warehouse_names)s) = 0
        OR array_contains(warehouse_name::variant, %(warehouse_names)s))
    GROUP BY 1
    ;
            """

        df = connection.execute_select(
            sql,
            {"start": bf.start, "end": bf.end, "warehouse_names": bf.warehouse_names},
        )

        with container:
            fig = go.Figure(
                data=[
                    go.Bar(
                        x=df["DT"],
                        y=df["COST"],
                        name="Cost",
                        yaxis="y",
                        offsetgroup=0,
                        marker_color="#0095F0",
                    ),
                    go.Bar(
                        x=df["DT"],
                        y=df["UTILIZATION"],
                        name="Utilization",
                        yaxis="y2",
                        offsetgroup=2,
                        marker_color="#856CF3",
                    ),
                ],
                layout={
                    "yaxis": {"title": "Cost", "tickprefix": "$", "tickformat": ",.2f"},
                    "yaxis2": {
                        "title": "Utilization",
                        "overlaying": "y",
                        "side": "right",
                        "range": [0, 1],
                        "tickformat": ",.0%",
                    },
                    "xaxis": {
                        "dtick": bf.dtick(),
                        "title": bf.ticktitle(),
                    },  # 'tickformat': '%b',
                    "legend": {"xanchor": "right", "x": 0.8},
                },
            )
            st.header("Warehouse Cost and Utilization")
            st.plotly_chart(fig, use_container_width=True)

    def warehouse_durations(container):
        sql = """
            select
                internal.friendly_duration(duration) as duration,
                internal.friendly_duration_ordinal(duration) as ord,
                count(1) as cnt
            from REPORTING.WAREHOUSE_SESSIONS
            where st between %(start)s and %(end)s and (array_size(%(warehouse_names)s) = 0
                OR array_contains(warehouse_name::variant, %(warehouse_names)s))
            group by 1, ord
            order by ord asc
            """
        df = connection.execute_select(
            sql,
            {"start": bf.start, "end": bf.end, "warehouse_names": bf.warehouse_names},
        )

        with container:
            fig = go.Figure(
                data=[
                    go.Bar(
                        x=df["DURATION"], y=df["CNT"], yaxis="y", marker_color="#ED5191"
                    ),
                ],
                layout={
                    "yaxis": {"title": "Session Count"},
                    "xaxis": {"title": "Duration of Warehouse Session"},
                },
            )
            st.header("Warehouse Running Duration")
            st.plotly_chart(fig, use_container_width=True)

    def warehouse_sleeps(container):
        sql = """
    with durations AS (
    SELECT
      warehouse_id,
      warehouse_name,
      st,
      datediff('seconds', LAG(st) OVER (PARTITION BY warehouse_id ORDER BY st), st)  AS duration
    FROM
      reporting.warehouse_sessions
    )
    select
        internal.friendly_duration(duration*1000) as duration,
        internal.friendly_duration_ordinal(duration*1000) as ord,
        count(1) as cnt
    from durations
        where st between %(start)s and %(end)s
            and (array_size(%(warehouse_names)s) = 0
            OR array_contains(warehouse_name::variant, %(warehouse_names)s))
    group by 1, ord
    order by ord asc
            """
        df = connection.execute_select(
            sql,
            {"start": bf.start, "end": bf.end, "warehouse_names": bf.warehouse_names},
        )

        with container:
            fig = go.Figure(
                data=[
                    go.Bar(
                        x=df["DURATION"], y=df["CNT"], yaxis="y", marker_color="#49C9C3"
                    ),
                ],
                layout={
                    "yaxis": {"title": "Session Count"},
                    "xaxis": {"title": "Time before Warehouse Start"},
                },
            )
            st.header("Warehouse Sleeping Duration")
            st.plotly_chart(fig, use_container_width=True)

    def warehouse_users(container):
        sql = """
            select warehouse_name, st_period, count(distinct user_name) cnt
            from reporting.enriched_query_history_daily
            where st_period between %(start)s and %(end)s and (array_size(%(warehouse_names)s) = 0
                OR array_contains(warehouse_name::variant, %(warehouse_names)s))
            group by warehouse_id, warehouse_name, st_period;
            """

        df = connection.execute(
            sql,
            {"start": bf.start, "end": bf.end, "warehouse_names": bf.warehouse_names},
        )
        fig = go.Figure()

        # Get the unique names of the warehouses
        warehouses = df["WAREHOUSE_NAME"].unique()

        # For each warehouse, create a trace and add it to the figure
        for warehouse in warehouses:
            df_warehouse = df[df["WAREHOUSE_NAME"] == warehouse]
            fig.add_trace(
                go.Scatter(
                    x=df_warehouse["ST_PERIOD"],
                    y=df_warehouse["CNT"],
                    mode="lines",
                    name=warehouse,
                )
            )

        # Set the layout of the figure
        fig.update_layout(
            title="Count per day by Warehouse",
            xaxis_title="Date",
            yaxis_title="Count",
            hovermode="x",
        )

        st.plotly_chart(fig, use_container_width=True)

    page_view("Warehouse Activity")
    warehouse_stats(st.empty())
    cols = st.columns(2)
    with cols[0]:
        warehouse_durations(st.empty())
    with cols[1]:
        warehouse_sleeps(st.empty())
