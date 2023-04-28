import streamlit as st
import connection
import pandas as pd
import plotly.figure_factory as ff
import calendar
import numpy as np
import filters


def heatmap(
    bf: filters.BaseFilter,
    cost_per_credit,
):

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
    df["UTILIZATION"] = (df["UTILIZATION"].astype(float) * 100).round(1)

    df["week"] = df["PERIOD"].dt.isocalendar().week
    df["year"] = df["PERIOD"].dt.isocalendar().year
    df["weekday"] = df["PERIOD"].dt.weekday

    # Convert weekday numbers to weekday names
    df["weekday"] = df["weekday"].apply(lambda x: calendar.day_name[x])

    # Sort by year and week
    df = df.sort_values(["year", "week"])

    day_order = [
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
    ]
    df["weekday"] = pd.Categorical(df["weekday"], categories=day_order, ordered=True)

    pivot_df = df.pivot_table(
        values="UTILIZATION",
        index=["year", "week"],
        columns="weekday",
        aggfunc="first",
        fill_value=None,
        dropna=False,
    )
    pivot_df = pivot_df.fillna(np.nan)

    # Create labels for the heatmap (short date format)
    df["short_date"] = df["PERIOD"].dt.strftime("%m/%d")
    pivot_df_labels = df.pivot_table(
        values="short_date",
        index=["year", "week"],
        columns="weekday",
        aggfunc="first",
        fill_value=None,
        dropna=False,
    )

    pivot_df = pivot_df.sort_index(ascending=False)

    pivot_df_utilization_labels = pivot_df.applymap(
        lambda x: f"{x}%" if pd.notnull(x) else ""
    )

    # Create the heatmap with customized colorscale and hovertext
    fig = ff.create_annotated_heatmap(
        z=pivot_df.values,
        x=list(pivot_df.columns),
        y=[f"Week {idx[1]} {idx[0]}" for idx in pivot_df.index],
        annotation_text=pivot_df_utilization_labels.values,
        text=pivot_df_labels.values,
        hovertemplate="<extra></extra>%{text}<br>Utilization: %{z}%",
    )

    fig["data"][0]["showscale"] = True
    fig["data"][0]["colorscale"] = [
        [0, "#E0D9FC"],
        [1, "#562FEE"],
    ]  # darker shade of lavender

    fig.update_layout(
        title="Weekly Utilization Heatmap",
        xaxis_title="Day of Week",
        yaxis_title="Week and Year",
        width=1000,
        height=1000,
    )

    st.plotly_chart(fig, use_container_width=True)
