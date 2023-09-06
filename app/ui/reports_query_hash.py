import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

import connection
import filters


def report(
    bf: filters.BaseFilter,
    cost_per_credit,
):
    _ = connection.execute(
        "CALL INTERNAL.REPORT_PAGE_VIEW('Query Report Repeated Queries')"
    )

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
        with agg as (
select sum(cost) as cost, count(*) as cnt, any_value(query_text) as query, query_parameterized_hash
from reporting.labeled_query_history where query_parameterized_hash is not null and cost >0
        and start_time between %(start)s and %(end)s and (array_size(%(warehouse_names)s) = 0
            OR array_contains(warehouse_name::variant, %(warehouse_names)s))
        {addition_filter}
group by query_parameterized_hash
), buckets as (
select sum(cost) as cost, sum(cnt) as cnt, width_bucket(log(10, cnt), 0, 7, 7) as bucket from agg group by all
)
select cost as "Cost", cnt as "Count", '[' || pow(10,bucket-1) || ', ' || pow(10,bucket) || ')' as "Bucket" from buckets order by "Bucket" desc
        """

    def overview():
        df = connection.execute_select(
            sql + " ;",
            {"start": bf.start, "end": bf.end, "warehouse_names": bf.warehouse_names},
        )

        labels = list(df.Bucket.values)

        fig = make_subplots(
            rows=1, cols=2, specs=[[{"type": "domain"}, {"type": "domain"}]]
        )
        fig.add_trace(
            go.Pie(labels=labels, values=df.Cost.values, name="Cost per bucket"), 1, 1
        )
        fig.add_trace(
            go.Pie(labels=labels, values=df.Count.values, name="Count per bucket"), 1, 2
        )

        fig.update_traces(hole=0.4, hoverinfo="label+percent+name")

        fig.update_layout(
            title_text="Repeated queries per frequency bucket",
            # Add annotations in the center of the donut pies.
            annotations=[
                dict(text="Cost", x=0.20, y=0.5, font_size=20, showarrow=False),
                dict(text="Count", x=0.81, y=0.5, font_size=20, showarrow=False),
            ],
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown(
        """
    The below pie charts show the distribution of repeated queries by frequency bucket. Queries are bucketed by the
    number of times their query hash has been seen in the time period selected. For example, the bucket `[1,10)` means
    all queries whose query hash has been seen <10 times and `[1000,10000)` means the query has has been seen between
    1000 and 10,000 times in the given time frame. The pie charts show the percentage contribution of each bucket to
    both total cost and to the overall query count.

    These charts indicate wether the primary cost driver is a small number of very expensive queries or a large number
    of repeating inexpensive queries. The charts can be filtered by labels and by warehouses to narrow down the source
    of these costs and improve them. For example, maybe the small repeated queries could be directed to a smaller
    warehouse.
    """
    )
    overview()
    sql = f"""
        with raw as (
        select any_value(query_text) as "Query Text", sum(cost) as "Cost", count(*) as "Count", query_parameterized_hash as "Parameterized Query Hash"
        from reporting.labeled_query_history where query_parameterized_hash is not null and cost >0
            and start_time between %(start)s and %(end)s and (array_size(%(warehouse_names)s) = 0
                OR array_contains(warehouse_name::variant, %(warehouse_names)s))
                {addition_filter}
                group by query_parameterized_hash
                order by "Count" desc
                ) select * from raw where length("Query Text") > 0
                limit 100
                """
    df = connection.execute_select(
        sql + ";",
        {"start": bf.start, "end": bf.end, "warehouse_names": bf.warehouse_names},
    )
    st.markdown(
        """
    Top 100 repeated queries by count.

    Tips:
    * double click on the query text to see the full query
    * copy the query hash and run `select * from opscenter.reporting.enriched_query_history where query_parameterized_hash = '<hash>'` to see the full query history for a particular hash
    """
    )
    st.dataframe(df, use_container_width=True)
