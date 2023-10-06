import streamlit as st
from modules import add_custom_modules
import connection
import session
import config
from table import build_table, Actions
from crud.base import BaseOpsCenterModel
from typing import ClassVar
from reports_heatmap import heatmap
from filters import BaseFilter

# Load custom OpsCenter python modules
if not add_custom_modules():
    st.warning("Unable to load OpsCenter modules.")


import warehouses  # noqa E402

sess = session.reports().get_report()


class WarehouseSummary(BaseOpsCenterModel):
    col_widths: ClassVar[dict] = {
        "warehouse": ("Warehouse", 2),
        "size": ("Size", 1),
        "autoscale": ("Autoscale", 0.5),
        "spend": ("Spend", 1),
        "utilization": ("Utilization", 1),
        "enabled": ("Schedule Enabled", 0.75),
    }
    warehouse: str
    size: str
    autoscale: bool
    spend: str
    utilization: str
    enabled: bool


def display():
    st.title("Manage Warehouses")
    filter_container = st.expander(
        "Filters", expanded="warehouse" not in st.session_state
    )
    with filter_container:
        s1, s2 = st.columns(2)

        with s1:
            range = st.date_input(
                "Date Range", value=[sess.get_report_start(), sess.get_report_end()]
            )
        with s2:
            cols = st.columns([1, 1, 1, 1])
            cols[0].button(
                "7", on_click=sess.set_date_range_days, args=[7], key="last7"
            )
            cols[1].button(
                "30", on_click=sess.set_date_range_days, args=[30], key="last30"
            )
            cols[2].button(
                "90", on_click=sess.set_date_range_days, args=[90], key="last90"
            )
            cols[3].button(
                "365", on_click=sess.set_date_range_days, args=[365], key="last365"
            )

        credit_cost = config.get_compute_credit_cost()
        sql = f"""
    begin
    show warehouses;
    with all_wh as (
    select "name" as warehouse_name, "size" as actual_size from table(result_scan(last_query_id()))
    ), util as (
    select warehouse_name, SUM(LOADED_CC * {credit_cost}) AS COST, IFF(SUM(LOADED_CC) = 0,null, SUM(UNLOADED_CC)/SUM(LOADED_CC)) AS UTILIZATION
     from reporting.warehouse_daily_utilization
            where PERIOD between %(start)s and %(end)s
            group by warehouse_name
     ), wh_raw as (
     select name as warehouse_name,
        any_value(enabled) as enabled,
        max(scale_max) > 1 as autoscale,
        any_value(size) as size,
        count(distinct size) as sz_count
    from internal.wh_schedules group by warehouse_name
     ), wh_agg as (
        select warehouse_name, enabled, autoscale, case when sz_count=1 then size else 'variable'end as size from wh_raw
     )
    select * from all_wh
     left join util on util.warehouse_name = all_wh.warehouse_name
      left join wh_agg on all_wh.warehouse_name = wh_agg.warehouse_name;
      let x resultset := (select warehouse_name as warehouse,
            coalesce(size, actual_size) as size,
            coalesce(autoscale, false) as autoscale,
            to_varchar(coalesce(cost, 0),'999999999.00') as spend,
            to_varchar(coalesce(utilization, 0)*100,'999.00') as utilization,
            coalesce(enabled, false) as enabled
        from table(result_scan(last_query_id())));
      return table(x);
    end;
     """

        sql = connection.Connection.bind(sql, {"start": range[0], "end": range[1]})
        df = connection.execute_with_cache(sql)
        data = WarehouseSummary.from_df(df)

        def set_warehouse(x):
            st.session_state["warehouse"] = x

        cbs = Actions(lambda x: set_warehouse(x), None, None, edit_icon="View & Manage")
        build_table(WarehouseSummary, data, cbs, has_empty=False, actions_size=4)

    if "warehouse" not in st.session_state:
        st.markdown(
            """No warehouse selected. Click 'View & Manage' above to manage a warehouse."""
        )
    else:
        whfilter = st.session_state["warehouse"]
        st.title(whfilter.warehouse)
        a, s, r = st.tabs(["Activity", "Schedule", "Recommendations"])
        with s:
            warehouses.display()
        with a:
            f = BaseFilter(None)
            f.start = range[0]
            f.end = range[1]
            f.warehouse_names = [whfilter.warehouse]
            heatmap(f, credit_cost)
        with r:
            st.markdown("Recommendations coming soon.")
