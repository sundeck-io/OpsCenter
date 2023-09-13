from plotly.subplots import make_subplots
import plotly.graph_objects as go
import streamlit as st

import connection
import filters


def report(
    bf: filters.BaseFilter,
    cost_per_credit,
):
    _ = connection.execute(
        "CALL INTERNAL.REPORT_PAGE_VIEW('Query Report Reoccurring Queries')"
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
select sum(cost) as cost, count(*) as cnt, query_parameterized_hash, year(end_time) || week(end_time) as end_time
from reporting.labeled_query_history where query_parameterized_hash is not null and cost >0
        and start_time between %(start)s and %(end)s and (array_size(%(warehouse_names)s) = 0
            OR array_contains(warehouse_name::variant, %(warehouse_names)s))
        {addition_filter}
group by all
), buckets as (
select *, cnt > 100 as "IsRepeated" from agg
)
select sum(cost) as "Cost", sum(cnt) as "Count", case when "IsRepeated" then 'Reoccurring' else 'ad-hoc' end as "Bucket" from buckets group by all
        """

    def overview():
        df = connection.execute_select(
            sql + " ;",
            {"start": bf.start, "end": bf.end, "warehouse_names": bf.warehouse_names},
        )

        fig = make_subplots(
            rows=1,
            cols=2,
            subplot_titles=["Cost", "Count"],
            specs=[[{"type": "domain"}, {"type": "domain"}]],
        )
        fig.add_trace(go.Pie(labels=df.Bucket, values=df.Cost, name="Cost"), 1, 1)
        fig.add_trace(go.Pie(labels=df.Bucket, values=df.Count, name="Count"), 1, 2)

        st.plotly_chart(fig, use_container_width=True)

    def top_table():
        val = st.radio(
            "Top 1000 repeated queries by:",
            ["Count", "Cost", "Cost per Query"],
        )
        sql = f"""
            with raw as (
            select any_value(query_text) as "Query Text", to_varchar(sum(cost), '999,999.00') as "Cost", count(*) as "Count", to_varchar(sum(cost)/count(*), '999,999.000000') as "Cost per Query"
            from reporting.labeled_query_history where query_parameterized_hash is not null and cost >0
                and start_time between %(start)s and %(end)s and (array_size(%(warehouse_names)s) = 0
                    OR array_contains(warehouse_name::variant, %(warehouse_names)s))
                    {addition_filter}
                    group by query_parameterized_hash
                    ) select * from raw where length("Query Text") > 0
                    order by "{val}" desc
                    limit 100
                    """
        df = connection.execute_select(
            sql + ";",
            {"start": bf.start, "end": bf.end, "warehouse_names": bf.warehouse_names},
        )
        st.markdown(
            """
        Tip: double click on the query text to see the full query.

        """
        )
        st.dataframe(df, use_container_width=True)

    is_enabled = connection.execute_select(
        """select system$BEHAVIOR_CHANGE_BUNDLE_STATUS('2023_06') = 'ENABLED' and count(*) = 1
        from information_schema.columns where table_catalog=current_database() and table_name='ENRICHED_QUERY_HISTORY' and table_schema='REPORTING' and column_name='QUERY_HASH';"""
    ).values[0][0]
    if not is_enabled:
        st.markdown(
            """
        **Note:** This report requires the `QUERY_HASH` column to be enabled in the `ENRICHED_QUERY_HISTORY` table.
        This column is enabled by default in Snowflake 2023.06. If you are on an earlier version, you can enable it
        by running the following command:

        ```
        BEGIN
          SELECT SYSTEM$ENABLE_BEHAVIOR_CHANGE_BUNDLE('2023_06');
          CALL opscenter.admin.finalize_setup();
        END;
        ```
        """
        )
    else:
        st.markdown(
            """
        The below charts show the ratio of reoccuring queries to ad-hoc queries. Reoccurring queries are defined as
        queries that have been run more than 100 times per week in the selected time range. Two ratios are shown: the
        contribution to cost for ad-hoc and reoccurring queries and the percentage of overall query count which is
        reoccurring or not.
        """
        )
        overview()
        top_table()
