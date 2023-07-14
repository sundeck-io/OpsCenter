import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import plotly.express as px
from reports_query_activity import topn
from telemetry import page_view

import connection
import filters


def report(
    bf: filters.BaseFilter,
    cost_per_credit,
):
    page_view("Query Report dbt Summary")

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

    sql = f"""
with raw as (
    select
        query_id,
        cost,
        date_trunc({bf.trunc()}, start_time) as d,
        value:KEY as dbt_qtag_name,
        value:VALUE as dbt_qtag_value, tools.model_run_time(total_elapsed_time) as run_time,
tools.model_size(tools.model_size_rows(zeroifnull(rows_produced)), tools.model_size_bytes(bytes_written_to_result)) as size,
tools.model_efficiency(bytes_spilled_to_local_storage, bytes_spilled_to_remote_storage) as efficiency
    from reporting.enriched_query_history, lateral flatten(qtag)
    where
        value:SOURCE = 'dbt'
        and (value:KEY = 'invocation_id' or value:KEY = 'node_id')
        and start_time between %(start)s and %(end)s and (array_size(%(warehouse_names)s) = 0
            OR array_contains(warehouse_name::variant, %(warehouse_names)s))
        {addition_filter}
    order by query_id
),

agg as (
    select
        dbt_qtag_value as k2,
        d,
        cost, run_time, size, efficiency,
        lag(dbt_qtag_value)
            over (partition by query_id order by dbt_qtag_name)
            as k1
    from raw
    where cost is not null
)

select
    d as "Date",
    cost as "Cost",
    1 as "Count",
    k2 as "ModelId",
    coalesce(k1, 'Unknown') as "RunId", run_time as "RunTimeGrade", size as "SizeGrade", efficiency as "EfficiencyGrade"
from agg
where
    not (k2 ilike '________-____-____-____-____________' and regexp_like(k2, '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$', 'i'));
        """

    def overview():
        df = connection.execute_select(
            sql,
            {"start": bf.start, "end": bf.end, "warehouse_names": bf.warehouse_names},
        )
        df["ModelId"] = df["ModelId"].map(lambda x: x.replace('"', ""))
        dfc = (
            df.groupby(["Date"]).agg({"Cost": "sum", "RunId": "nunique"}).reset_index()
        )

        # Create figure with secondary y-axis
        fig_cost = go.Figure(
            data=[
                go.Bar(
                    x=dfc["Date"],
                    y=dfc["Cost"],
                    name="Cost",
                    yaxis="y",
                    offsetgroup=0,
                    marker_color="#0095F0",
                ),
                go.Scatter(
                    x=dfc["Date"],
                    y=dfc["RunId"],
                    name="Run Count",
                    yaxis="y2",
                    marker_color="#856CF3",
                ),
            ],
            layout={
                "yaxis": {"title": "Cost", "tickprefix": "$", "tickformat": ",.2f"},
                "yaxis2": {
                    "title": "Number of Runs",
                    "overlaying": "y",
                    "side": "right",
                },
                "xaxis": {"dtick": bf.dtick(), "title": bf.ticktitle()},
                "legend": {"xanchor": "right", "x": 0.2},
            },
        )
        fig_cost.update_layout({"title": "Cost and Number of Runs"})

        st.plotly_chart(fig_cost, use_container_width=True)

        dfm = (
            df.groupby(["ModelId"])
            .agg({"Cost": "sum", "RunId": "nunique"})
            .reset_index()
            .sort_values(by=["Cost"], ascending=True)
        )
        dfm = topn(dfm[dfm.Cost > 0.5], 20, {"Cost": "sum"}, "ModelId", False)

        fig = px.bar(dfm[dfm.Cost > 0.5], x="Cost", y="ModelId", orientation="h")
        fig.update_layout({"title": "Model Cost"})
        st.plotly_chart(fig, use_container_width=True)

        dfh = create_heatmap(df[df.ModelId.isin(dfm.ModelId)])
        st.markdown(
            """
        Below rankings are based on the criteria from the [GitLab dbt
        Manual](https://about.gitlab.com/handbook/business-technology/data-team/platform/dbt-guide/#model-performance)
        and take into account the following:
        * Run time
        * Output table size
        * Output table rows
        * Amount spilled to disk
        """
        )
        st.dataframe(dfh, use_container_width=True)

    view = st.selectbox("Pick View", pd.DataFrame({"options": ["Graph", "List"]}))

    st.markdown(
        """
    The below graphs show dbt model performance metrics. To use ensure that you have followed
    [these instructions](https://docs.getdbt.com/reference/project-configs/query-comment#append-the-default-comment) to
    get per model qtags and [this
    quickstart](https://github.com/get-select/dbt-snowflake-query-tags#quickstart) to get the run count.
    """
    )
    if view == "Graph":
        overview()
    else:
        df = connection.execute_select(
            sql + " limit 1000",
            {"start": bf.start, "end": bf.end, "warehouse_names": bf.warehouse_names},
        )
        st.dataframe(df, use_container_width=True)


def create_heatmap(df: pd.DataFrame) -> pd.DataFrame:
    sorted_df = df.sort_values(by=["ModelId", "Date"])
    grades = ["XS", "S", "M", "L", "XL", "XL+"]
    grades_e = ["Good", "Acceptable", "Poor", "VeryPoor"]
    sorted_df.SizeGrade = pd.Categorical(sorted_df.SizeGrade.values, categories=grades)
    sorted_df.RunTimeGrade = pd.Categorical(
        sorted_df.RunTimeGrade.values, categories=grades
    )
    sorted_df.EfficiencyGrade = pd.Categorical(
        sorted_df.EfficiencyGrade.values, categories=grades_e
    )
    a1 = (
        sorted_df.groupby(["ModelId"])
        .agg(
            {
                "SizeGrade": lambda x: trendline(x.cat.codes),
                "RunTimeGrade": lambda x: trendline(x.cat.codes),
                "EfficiencyGrade": lambda x: trendline(x.cat.codes),
            }
        )
        .rename(
            columns={
                "SizeGrade": "SizeTrend",
                "RunTimeGrade": "RunTimeTrend",
                "EfficiencyGrade": "EfficiencyTrend",
            }
        )
    )
    a2 = sorted_df.groupby(["ModelId"]).last()
    dd = pd.concat([a1, a2], axis=1)
    dd["Size (Trend)"] = dd.apply(
        lambda x: f"{x['SizeGrade']} ({x['SizeTrend']})", axis=1
    )
    dd["Run Time (Trend)"] = dd.apply(
        lambda x: f"{x['RunTimeGrade']} ({x['RunTimeTrend']})", axis=1
    )
    dd["Efficiency (Trend)"] = dd.apply(
        lambda x: f"{x['EfficiencyGrade']} ({x['EfficiencyTrend']})", axis=1
    )
    dd = dd[["Size (Trend)", "Run Time (Trend)", "Efficiency (Trend)"]]
    return dd


def trendline(x: pd.Series, n: int = 10) -> str:
    t = x.tail(n).diff().dropna().sum()
    if t < 0:
        return "⬇️"
    if t > 0:
        return "⬆️"
    return "➖"
