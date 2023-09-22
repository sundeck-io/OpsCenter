import streamlit as st
import connection
from crud.wh_sched import WarehouseSchedules, _WAREHOUSE_SIZE_OPTIONS
from table import build_table, Actions
from typing import Optional, List
import datetime
from base import Container
from warehouse_utils import (
    time_filter,
    create_callback,
    convert_time_str,
    verify_and_clean,
    populate_initial,
    flip_enabled,
)

# exclude cols from table
def display():
    Warehouses.display()


class Warehouses(Container):
    base_cls = WarehouseSchedules
    ui_name: Optional[str] = "Warehouse Schedule"

    def create_internal(self, create):
        current, data = create
        if current.name == "" and current.size == "":
            # new bottom row
            row = data[-1]
        else:
            # insert row
            row = current
        i = data.index(row)
        min_start = data[i].start_at if i != 0 else datetime.time(0, 0)

        start_time_filter = time_filter(datetime.time(23, 59), min_start, True)
        finish_time_filter = time_filter(datetime.time(23, 59), min_start, False)
        new_row = self.form(
            row,
            start_time_filter,
            finish_time_filter,
            is_create=True,
        )
        return new_row, data, current

    def edit_internal(self, update_obj):
        update, data = update_obj
        i = data.index(update)
        # todo, not sure if this is quite right
        min_start = data[i - 1].start_at if i > 0 else datetime.time(0, 0)
        edit_start = update.start_at != datetime.time(0, 0)
        if edit_start:
            start_time_filter = time_filter(datetime.time(23, 59), min_start, True)
        else:
            start_time_filter = [min_start.strftime("%I:%M %p")]
        finish_time_filter = time_filter(datetime.time(23, 59), min_start, False)
        new_update = self.form(update, start_time_filter, finish_time_filter)
        return update, new_update

    def form(
        self,
        update,
        start_time_filter: List[str] = None,
        finish_time_filter: List[str] = None,
        is_create=False,
    ):
        size = st.selectbox(
            key="SIZE",
            label="Size",
            options=_WAREHOUSE_SIZE_OPTIONS,
        )
        start = st.selectbox(
            key="START",
            label="Start",
            options=start_time_filter,
            disabled=len(start_time_filter) == 1,
        )
        finish = st.selectbox(
            key="FINISH",
            label="Finish",
            options=finish_time_filter,
            index=(len(finish_time_filter) - 1)
            if update.finish_at == datetime.time.max
            else next(
                i
                for i, v in enumerate(finish_time_filter)
                if v == update.finish_at.strftime("%I:%M %p")
            ),
            disabled=True,
        )
        suspend_minutes = st.number_input(
            key="SUSPEND_MINUTES",
            label="Suspend Minutes",
            value=update.suspend_minutes,
            min_value=0,
        )
        autoscale_mode = st.radio(
            key="AUTOSCALE_MODE",
            label="Autoscale Mode",
            options=("Inherit", "Standard", "Economy"),
            index=0 if is_create else 1 if update.warehouse_mode == "Standard" else 2,
            horizontal=True,
        )
        # TODO make sure min and max are set correctly
        autoscale_min = st.number_input(
            key="AUTOSCALE_MIN",
            label="Min Clusters",
            value=max(update.scale_min, 1),
            disabled=update.scale_min == 0,
            min_value=1,
            max_value=update.scale_max,
        )
        autoscale_max = st.number_input(
            key="AUTOSCALE_MAX",
            label="Max Clusters",
            value=max(update.scale_max, 1),
            disabled=update.scale_max == 0,
            min_value=autoscale_min,
        )
        comment = st.text_input(
            key="COMMENT",
            label="Comment",
            value=update.comment if update.comment is not None else "",
        )
        resume = st.checkbox(
            key="RESUME",
            label="Auto Resume",
            value=update.resume,
        )

        new_update = WarehouseSchedules(
            name=update.name,
            size=size,
            suspend_minutes=suspend_minutes,
            resume=resume,
            scale_min=autoscale_min,
            scale_max=autoscale_max,
            warehouse_mode=autoscale_mode,
            comment=comment if comment != "" else None,
            start_at=convert_time_str(start),
            finish_at=convert_time_str(finish),
        )
        return new_update

    def on_create_click_internal(self, *args) -> Optional[str]:
        row, data, current = args
        if current.name == "" and current.size == "":
            # new bottom row
            data.append(row)
        else:
            # new middle row
            i = data.index(current)
            data.insert(i + 1, row)

        outcome, new_data = verify_and_clean(data)
        if outcome is not None:
            return outcome
        old_warehouses = WarehouseSchedules.batch_read(connection.Connection.get())
        old_warehouses_new = [i for i in old_warehouses if i.name != row.name]
        old_warehouses_we = [
            i for i in old_warehouses if i.name == row.name and i.weekday != row.weekday
        ]
        new_warehouses = old_warehouses_new + old_warehouses_we + new_data
        WarehouseSchedules.batch_write(connection.Connection.get(), new_warehouses)

    def on_delete_click_internal(self, *args) -> Optional[str]:
        warehouses = WarehouseSchedules.batch_read(connection.Connection.get())
        i = warehouses.index(args[0])
        del warehouses[i]
        comment, new_warehouses = verify_and_clean(warehouses, ignore_errors=True)
        if comment is not None:
            return comment
        WarehouseSchedules.batch_write(connection.Connection.get(), new_warehouses)

    def on_update_click_internal(self, *args) -> Optional[str]:
        warehouses = WarehouseSchedules.batch_read(connection.Connection.get())
        i = warehouses.index(args[0])
        warehouses[i] = args[1]
        comment, new_warehouses = verify_and_clean(warehouses)
        if comment is not None:
            return comment
        WarehouseSchedules.batch_write(connection.Connection.get(), new_warehouses)

    def list(self):
        st.button(
            "Clear State",
            on_click=lambda: connection.Connection.get().sql(
                f"delete from internal.{WarehouseSchedules.table_name}"
            ),
        )
        warehouses = connection.execute_with_cache(
            """
        begin
            show warehouses;
            let res resultset := (select "name" from table(result_scan(last_query_id())) order by "name");
            return table(res);
        end;
                                                   """
        )
        whfilter = st.selectbox("Warehouse Filter", options=warehouses, key="whfilter")
        populate_initial(whfilter)
        all_data = WarehouseSchedules.batch_read(connection.Connection.get())

        data = [i for i in all_data if i.weekday and i.name == whfilter]
        data_we = [i for i in all_data if not i.weekday and i.name == whfilter]

        is_enabled = all(i.enabled for i in data) and all(i.enabled for i in data_we)
        st.checkbox(
            "Enable Schedule",
            value=is_enabled,
            key="enabled",
            on_change=lambda: flip_enabled(data) + flip_enabled(data_we),
        )

        st.title("Weekdays")
        cbs = Actions(
            lambda x: self.session.do_edit(create_callback(data, x, weekday=True)),
            self.on_delete_click,
            lambda row: self.session.do_create(
                create_callback(data, row, weekday=True)
            ),
        )
        build_table(self.base_cls, data, cbs, has_empty=True)
        st.title("Weekends")
        cbs = Actions(
            lambda row: self.session.do_edit(
                create_callback(data_we, row, weekday=False)
            ),
            self.on_delete_click,
            lambda row: self.session.do_create(
                create_callback(data_we, row, weekday=False)
            ),
        )
        build_table(self.base_cls, data_we, cbs, has_empty=True)

        st.write(WarehouseSchedules.batch_read(connection.Connection.get()))
