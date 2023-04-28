from typing import List, Union, Any

import streamlit as st
from streamlit.elements.time_widgets import SingleDateValue

import connection
import session

sess = session.reports().get_report()


class BaseFilter:
    start: Union[SingleDateValue, None]
    end: Union[SingleDateValue, None]
    warehouse_names: List[str]
    container: Any

    def __init__(self, container):
        self.start = None
        self.end = None
        self.warehouse_names = []
        self.container = container

    def valid(self) -> bool:
        return self.start is not None and self.end is not None

    def days(self) -> int:
        return (self.end - self.start).days

    def is_monthly(self) -> bool:
        return self.days() > 31

    def dtick(self):
        return "M1" if self.is_monthly() else "D1"

    def ticktitle(self):
        return "Month" if self.is_monthly() else "Day"

    def tbl(self):
        return "daily" if self.is_monthly() else "daily"

    def trunc(self):
        return "month" if self.is_monthly() else "day"


def display(container) -> BaseFilter:
    with container:
        # Setup columns
        s1, s2 = st.columns(2)

        with s1:
            range = st.date_input(
                "Date Range", value=[sess.get_report_start(), sess.get_report_end()]
            )
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

        warehouses = connection.execute_with_cache(
            "select distinct warehouse_name from reporting.warehouse_sessions union all select 'Serverless Task'"
        )
        if len(warehouses) > 0:
            with s2:
                whfilter = s2.multiselect(
                    "Warehouse Filter", options=warehouses, key="whfilter"
                )
        else:
            whfilter = []

    bf = BaseFilter(container)
    bf.warehouse_names = whfilter

    # Don't try to show data unless both ends of range are selected.
    if len(range) != 2:
        return bf

    bf.start = range[0]
    bf.end = range[1]
    return bf
