from session import Mode
import streamlit as st
from connection import Connection
import session as general_session
import connection
from wh_sched import WarehouseSchedules
from base import BaseOpsCenterModel
from table import build_table, Actions
from typing import Optional, List, Tuple
import datetime

# exclude cols from table
def display():
    Warehouses.display()


class Container:
    base_cls: Optional[BaseOpsCenterModel] = None
    ui_name: Optional[str] = None

    @classmethod
    def display(cls):
        container = cls()

        if container.session.mode is Mode.LIST:
            container.list()
        elif container.session.mode is Mode.CREATE:
            container.create(container.session.create)
        elif container.session.mode is Mode.EDIT:
            container.edit(container.session.update)
        else:
            st.exception("Mode not detected.")
            st.text(container.session)

    def __init__(self):
        self.status = st.empty()
        self.session = general_session.Sessions.get(self.base_cls.table_name)
        self.session.show_toast(self.status)

        self.snowflake = Connection.get()

    def list(self):
        pass

    def create(self, create):
        pass

    def edit(self, update):
        pass

    def on_update_click_internal(self, *args) -> Optional[str]:
        pass

    def on_delete_click_internal(self, *args) -> Optional[str]:
        pass

    def on_create_click_internal(self, *args) -> Optional[str]:
        pass

    def on_create_click(self, *args):
        with st.spinner("Creating new f{self.ui_name.lower()}..."):
            _ = self.snowflake.call(
                "INTERNAL.REPORT_ACTION",
                f"{self.ui_name.lower()}s",
                "create",
            )
            outcome = self.on_create_click_internal(*args)
            if outcome is None:
                self.session.set_toast(f"New {self.ui_name.lower()} created.")
                self.session.do_list()
                return

        self.status.error(outcome)

    def on_delete_click(self, *args):
        with st.spinner("Deleting label..."):
            _ = self.snowflake.call(
                "INTERNAL.REPORT_ACTION",
                f"{self.ui_name.lower()}s",
                "delete",
            )
            self.on_delete_click_internal(*args)
            self.session.set_toast(f"{self.ui_name} deleted.")
            self.session.do_list()

    def on_update_click(self, *args):
        with st.spinner(f"Updating {self.ui_name.lower()}..."):
            _ = self.snowflake.call(
                "INTERNAL.REPORT_ACTION",
                f"{self.ui_name.lower()}s",
                "update",
            )
            outcome = self.on_update_click_internal(*args)

            if outcome is None:
                self.session.set_toast(f"{self.ui_name} updated.")
                self.session.do_list()
                return

        self.status.error(outcome)


def create_callback(data, row, **additions):
    if row is None:
        row = WarehouseSchedules(
            name="",
            size="",
            suspend_minutes=0,
            resume=True,
            scale_min=1,
            scale_max=1,
            warehouse_mode="",
        )
    for k, v in additions.items():
        setattr(row, k, v)
    return (row, data)


def populate_initial(warehouse):
    WarehouseSchedules.create_table(connection.Connection.get())
    warehouses = WarehouseSchedules.batch_read(connection.Connection.get())
    if any(i for i in warehouses if i.name == warehouse) == 0:
        wh = describe_warehouse(warehouse)
        warehouses.append(wh)
        wh = describe_warehouse(warehouse)
        wh.weekday = False
        warehouses.append(wh)
    WarehouseSchedules.batch_write(connection.Connection.get(), warehouses)


def describe_warehouse(warehouse):
    wh_df = connection.execute(f"show warehouses like '{warehouse}'")
    wh_dict = wh_df.T[0].to_dict()
    return WarehouseSchedules(
        name=warehouse,
        size=wh_dict["size"],
        suspend_minutes=int(wh_dict["auto_suspend"] or 0) // 60,
        resume=wh_dict["auto_resume"],
        scale_min=wh_dict.get("min_cluster_count", 0),
        scale_max=wh_dict.get("max_cluster_count", 0),
        warehouse_mode=wh_dict.get("scaling_policy", "Standard"),
    )


def convert_time_str(time_str) -> datetime.time:
    return datetime.datetime.strptime(time_str, "%I:%M %p").time()


def verify_and_clean(
    data: List[WarehouseSchedules], ignore_errors=False
) -> Tuple[Optional[str], List[WarehouseSchedules]]:
    if data[0].start_at != datetime.time(0, 0):
        if ignore_errors:
            data[0].start_at = datetime.time(0, 0)
        else:
            return "First row must start at midnight.", data
    if data[-1].finish_at != datetime.time(23, 59):
        if ignore_errors:
            data[-1].finish_at = datetime.time(23, 59)
        else:
            return "Last row must end at midnight.", data
    next_start = data[0]
    for row in data[1:]:
        if row.start_at != next_start.finish_at:
            next_start.finish_at = row.start_at
        if row.warehouse_mode == "Inherit":
            row.warehouse_mode = next_start.warehouse_mode
        next_start = row
    return None, data


class Warehouses(Container):
    base_cls = WarehouseSchedules
    ui_name: Optional[str] = "Warehouse"

    # TODO: can we genericize this? Eg pass in a list of fields to create and the component to use?
    def create(self, create):
        current, data = create
        if current.name == "" and current.size == "":
            # new bottom row
            row = data[-1]
        else:
            # insert row
            row = current
        st.title("Create Warehouse Schedule")
        i = data.index(row)
        min_start = data[i].start_at if i != 0 else datetime.time(0, 0)
        try:
            max_finish = data[i + 1].start_at
        except IndexError:
            max_finish = datetime.time(23, 59)
        new_row = self.form(
            row,
            is_create=True,
            edit_start=True,
            min_start=min_start,
            max_finish=max_finish,
        )
        st.button(
            "Create",
            on_click=self.on_create_click,
            args=[new_row, data, current],
        )
        st.button("Cancel", on_click=self.session.do_list)

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

    # TODO: can we genericize this? Eg pass in a list of fields to edit and the component to use?
    def edit(self, update_obj):
        update, data = update_obj
        i = data.index(update)
        # todo, not sure if this is quite right
        min_start = data[i - 1].start_at if i > 0 else datetime.time(0, 0)
        edit_start = update.start_at != datetime.time(0, 0)
        st.title("Edit Warehouse Schedule")
        new_update = self.form(update, edit_start=edit_start, min_start=min_start)
        st.button(
            "Update",
            on_click=self.on_update_click,
            args=[update, new_update],
        )
        st.button("Cancel", on_click=self.session.do_list)

    def form(
        self,
        update,
        max_finish: datetime.time = datetime.time(23, 59),
        min_start: datetime.time = datetime.time(0, 0),
        edit_start=False,
        is_create=False,
    ):
        size = st.selectbox(
            key="SIZE",
            label="Size",
            options=_WAREHOUSE_SIZE_OPTIONS,
        )
        start_time_filter = time_filter(max_finish, min_start, True)
        start = st.selectbox(
            key="START",
            label="Start",
            options=start_time_filter,
            disabled=not edit_start,
        )
        finish_time_filter = time_filter(max_finish, min_start, False)
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


def flip_enabled(data: List[WarehouseSchedules]) -> List[WarehouseSchedules]:
    for i in data:
        i.enabled = not i.enabled
    return data


def time_filter(
    max_finish: datetime.time, min_start: datetime.time, is_start: bool
) -> List[str]:
    hours = [12] + list(range(1, 12))
    minutes = list(range(0, 60, 15))
    ampm = ["AM", "PM"]
    times = [f"{h:02}:{m:02} {a}" for a in ampm for h in hours for m in minutes]
    base_times = times + ["11:59 PM"]
    if is_start:
        return [
            i
            for i in base_times
            if convert_time_str(i) > min_start and convert_time_str(i) < max_finish
        ]
    else:
        return base_times


_WAREHOUSE_SIZE_OPTIONS = [
    "X-Small",
    "Small",
    "Medium",
    "Large",
    "X-Large",
    "2X-Large",
    "3X-Large",
    "4X-Large",
    "5X-Large",
    "6X-Large",
    "Medium Snowpark",
    "Large Snowpark",
    "X-Large Snowpark",
    "2X-Large Snowpark",
    "3X-Large Snowpark",
    "4X-Large Snowpark",
]
