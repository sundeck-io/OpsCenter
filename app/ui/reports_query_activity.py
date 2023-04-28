import pandas as pd
import streamlit as st
import filters
import connection
import plotly.express as px


def report(
    bf: filters.BaseFilter,
    cost_per_credit,
):
    labels = connection.execute_select(
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

    groups = pd.concat(
        [
            pd.DataFrame(
                {
                    "Group": [
                        "User",
                        "Warehouse",
                        "Role",
                        "Query Type",
                        "Execution Status",
                    ]
                }
            ),
            connection.execute_select(
                """select distinct group_name as "Group" from internal.labels where group_name is not null"""
            ),
        ]
    )

    grouping = st.selectbox("Color by Category or Grouping Label", groups)
    if grouping == "User":
        grp = "USER_NAME"
    elif grouping == "Warehouse":
        grp = "WAREHOUSE_NAME"
    elif grouping == "Role":
        grp = "ROLE_NAME"
    elif grouping == "Query Type":
        grp = "QUERY_TYPE"
    elif grouping == "Execution Status":
        grp = "EXECUTION_STATUS"
    else:
        grp = f""" "{grouping}" """

    def overview():
        sql = f"""
        select
            date_trunc({bf.trunc()}, start_time) as "Date", {grp} as "Group",
            sum(qh.unloaded_direct_compute_credits * {cost_per_credit}) as "Cost",
            count(*) as "Queries"
        from reporting.labeled_query_history qh
        where start_time between %(start)s and %(end)s and (array_size(%(warehouse_names)s) = 0
            OR array_contains(warehouse_name::variant, %(warehouse_names)s))
        {addition_filter}
        group by "Date", "Group"
        """
        df = connection.execute_select(
            sql,
            {"start": bf.start, "end": bf.end, "warehouse_names": bf.warehouse_names},
        )
        df = topn(df, 10, {"Cost": "sum", "Queries": "sum"})

        fig_cost = px.bar(
            df,
            x="Date",
            y="Cost",
            color="Group",
            barmode="stack",
            category_orders={"Group": df.Group.sort_values()},
        )

        fig_cost.update_layout(
            title="Query Cost",
            xaxis_dtick=bf.dtick(),
            xaxis_title=bf.ticktitle(),
            yaxis_title="Cost",
            showlegend=True,
        )
        st.plotly_chart(fig_cost, use_container_width=True)

        fig_count = px.bar(
            df,
            x="Date",
            y="Queries",
            color="Group",
            barmode="stack",
            category_orders={"Group": df.Group.sort_values()},
        )

        fig_count.update_layout(
            title="Query Count",
            xaxis_dtick=bf.dtick(),
            xaxis_title=bf.ticktitle(),
            yaxis_title="Queries",
            showlegend=True,
        )
        st.plotly_chart(fig_count, use_container_width=True)

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


def topn(
    df: pd.DataFrame,
    n: int,
    col_aggs: dict,
    rank_col_name: str = "Group",
    has_date: bool = True,
) -> pd.DataFrame:
    df["rank"] = df.groupby([rank_col_name]).ngroup().map(lambda x: min(x, n))
    df["NewGroup"] = df.apply(
        lambda x: x[rank_col_name] if x["rank"] < n else "Other", axis=1
    )
    date_list = ["Date"] if has_date else []
    df = df[["NewGroup"] + date_list + list(col_aggs.keys())]
    df.rename(columns={"NewGroup": rank_col_name}, inplace=True)
    return (
        df.groupby([rank_col_name] + date_list)
        .agg(col_aggs)
        .reset_index()
        .sort_values(by=list(col_aggs.keys()))
    )
