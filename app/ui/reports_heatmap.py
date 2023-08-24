import streamlit as st
import datetime
import connection
import pandas as pd
import plotly.express as px
import calendar
import numpy as np
import filters


def heatmap(
    bf: filters.BaseFilter,
    cost_per_credit,
):
    _ = connection.execute("CALL INTERNAL.REPORT_PAGE_VIEW('Warehouse Heatmap')")

    sql = f"""
    with util as (
        select date_trunc('DAY', PERIOD) AS PERIOD, SUM(LOADED_CC * {cost_per_credit}) AS COST, IFF(SUM(LOADED_CC) = 0,null, SUM(UNLOADED_CC)/SUM(LOADED_CC)) AS UTILIZATION
        from REPORTING.WAREHOUSE_DAILY_UTILIZATION
        where PERIOD between %(start)s and %(end)s and (array_size(%(warehouse_names)s) = 0 OR array_contains(warehouse_name::variant, %(warehouse_names)s))
        GROUP BY 1
    )

    select cast(d.date as timestamp) as PERIOD, UTILIZATION from
    internal.dates d
    left outer join util u on d.date = u.PERIOD
    where d.date between %(start)s and %(end)s
    """
    df = connection.execute(
        sql, {"start": bf.start, "end": bf.end, "warehouse_names": bf.warehouse_names}
    )
    df.set_index(["PERIOD"], inplace=True)
    low = df.index.min().date()
    high = df.index.max().date()

    df = df.reindex(
        pd.date_range(
            low - datetime.timedelta(days=low.weekday()),
            high - datetime.timedelta(days=low.weekday()) + datetime.timedelta(days=5),
            freq="D",
        ),
        fill_value=np.nan,
    )
    df.reset_index(inplace=True)
    df.rename(columns={"index": "PERIOD"}, inplace=True)
    df["UTILIZATION"] = (df["UTILIZATION"].astype(float) * 100).round(1)

    df["week"] = df["PERIOD"].dt.isocalendar().week
    df["year"] = df["PERIOD"].dt.isocalendar().year
    df["weekday"] = df["PERIOD"].dt.weekday

    # Convert weekday numbers to weekday names
    df["weekday"] = df["weekday"].apply(lambda x: calendar.day_name[x])

    # Sort by year and week
    df = df.sort_values(["year", "week"])

    day_order = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    df["weekday"] = pd.Categorical(df["weekday"], categories=day_order, ordered=True)

    df["short_date"] = df["PERIOD"].dt.strftime("%m/%d")

    pivot_df = df.groupby(["year", "week", "weekday"]).first().unstack()
    # pivot_df = pivot_df.fillna(np.nan)

    pivot_df = pivot_df.sort_index(ascending=True)

    pivot_df.UTILIZATION_TEXT = pivot_df.UTILIZATION.applymap(
        lambda x: f"{x}%" if pd.notnull(x) else ""
    )

    # Create the heatmap with customized colorscale and hovertext
    fig = px.imshow(
        pivot_df.UTILIZATION.values,
        x=list(pivot_df.UTILIZATION.columns),
        y=[f"Week {idx[1]} {idx[0]}" for idx in pivot_df.index],
        color_continuous_scale=[[0, "#E0D9FC"], [1, "#562FEE"]],
    )

    fig.update_traces(
        text=pivot_df.UTILIZATION_TEXT.values,
        texttemplate="%{text}",
        hovertemplate="<extra></extra>%{hovertext}<br>Utilization: %{z}%",
        hovertext=pivot_df.short_date.values,
    )
    fig["data"][0]["showscale"] = True
    fig.update_xaxes(side="top")

    fig.update_layout(
        title="Weekly Utilization Heatmap",
        xaxis_title="Day of Week",
        yaxis_title="Week and Year",
        width=1000,
        height=1000,
    )

    st.plotly_chart(fig, use_container_width=True)
