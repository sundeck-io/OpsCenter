import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import connection
import filters


def report(
    bf: filters.BaseFilter,
    cost_per_credit,
):
    labels = connection.execute_with_cache(
        "select name from internal.labels where group_name is null"
    )

    addition_filter = ""

    if len(labels) > 0:
        with bf.container:
            c1, c2, c3 = st.columns(3)
            with c1:
                include_all = st.multiselect("Include All", options=labels)
            with c2:
                include_any = st.multiselect("Include Any", options=labels)
            with c3:
                exclude_any = st.multiselect("Exclude Any", options=labels)

        for label in include_all:
            addition_filter += f""" and "{label}" """

        if len(include_any) > 0:
            addition_filter += " and ("
            for label in include_any:
                addition_filter += f""" "{label}" or """

            addition_filter = addition_filter[:-3] + ") "

        if len(exclude_any) > 0:
            addition_filter += " and not ("
            for label in exclude_any:
                addition_filter += f""" "{label}" or """

            addition_filter = addition_filter[:-3] + ") "

    def overview():
        sql = f"""
        select user_name, sum(cost) as cst, count(1) as queries from reporting.labeled_query_history qh
        where start_time between %(start)s and %(end)s and (array_size(%(warehouse_names)s) = 0
            OR array_contains(warehouse_name::variant, %(warehouse_names)s))
        {addition_filter}
        group by user_name having cst is not null order by cst desc
        """
        df = connection.execute_select(
            sql,
            {"start": bf.start, "end": bf.end, "warehouse_names": bf.warehouse_names},
        )

        fig = go.Figure(
            data=[
                go.Bar(
                    x=df["USER_NAME"],
                    y=df["CST"],
                    name="Cost",
                    yaxis="y",
                    offsetgroup=0,
                    marker_color="#0095F0",
                ),
                go.Bar(
                    x=df["USER_NAME"],
                    y=df["QUERIES"],
                    name="Queries",
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
                },
                "xaxis": {"dtick": bf.dtick(), "title": bf.ticktitle()},
                "legend": {"xanchor": "right", "x": 0.2},
            },
        )
        st.plotly_chart(fig, use_container_width=True)

    view = st.selectbox("Pick View", pd.DataFrame({"options": ["Graph", "List"]}))

    if view == "Graph":
        overview()
    else:
        sql = f"""
                select
                    start_time,
                    user_name,
                    query_text,
                    duration,
                    execution_status,
                    qh.unloaded_direct_compute_credits * {cost_per_credit} as COST
                from reporting.labeled_query_history qh
                where start_time between %(start)s and %(end)s and (array_size(%(warehouse_names)s) = 0
                    OR array_contains(warehouse_name::variant, %(warehouse_names)s))
                {addition_filter}
                limit 1000
                """
        df = connection.execute_select(
            sql,
            {"start": bf.start, "end": bf.end, "warehouse_names": bf.warehouse_names},
        )
        st.dataframe(df, use_container_width=True)
