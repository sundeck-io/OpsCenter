import uuid
import streamlit as st
import connection
from crud.base import transaction
from crud.wh_sched import (
    WarehouseSchedules,
    _WAREHOUSE_SIZE_OPTIONS,
    after_schedule_change,
    fetch_schedules_with_defaults,
    verify_and_clean,
)
from table import build_table, Actions
from typing import Optional, List
import datetime
from base import Container
from warehouse_utils import (
    time_filter,
    create_callback,
    convert_time_str,
    set_enabled,
)
from crud.errors import summarize_error

# exclude cols from table
def display():
    Warehouses.display()


class Warehouses(Container):
    base_cls = WarehouseSchedules
    ui_name: Optional[str] = "Warehouse Schedule"

    def create_internal(self, create):
        current, data = create
        if current.name == "__empty__placeholder__" and current.size == "X-Small":
            # new bottom row
            row = data[-1]
        else:
            # insert row
            row = current
        i = data.index(row)
        min_start = data[i].start_at if i != 0 else datetime.time(0, 0)
        try:
            max_finish = data[i + 1].start_at
        except IndexError:
            max_finish = datetime.time(23, 59)

        start_time_filter = time_filter(max_finish, min_start, True)
        finish_time_filter = time_filter(max_finish, min_start, False)
        try:
            new_row = self.form(
                row,
                start_time_filter,
                finish_time_filter,
                is_create=True,
            )
            comment = None
        except Exception as e:
            comment = summarize_error("Verify failed", e)
            new_row = None

        return new_row, data, current, comment

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
        try:
            new_update = self.form(update, start_time_filter, finish_time_filter)
            comment = None
        except Exception as e:
            comment = summarize_error("Verify failed", e)
            new_update = None

        return new_update, data, update, comment

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
            options=list(_WAREHOUSE_SIZE_OPTIONS.keys()),
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
            value=update.st_min_cluster_value(),
            disabled=not update.autoscaling_enabled(),
            min_value=update.st_min_cluster_minvalue(),
            max_value=update.scale_max,
        )
        autoscale_max = st.number_input(
            key="AUTOSCALE_MAX",
            label="Max Clusters",
            value=update.st_max_cluster_value(),
            disabled=not update.autoscaling_enabled(),
            min_value=autoscale_min,
            max_value=update.st_max_cluster_maxvalue(),
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

        new_update = WarehouseSchedules.parse_obj(
            dict(
                id_val=update.id_val,
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
                weekday=update.weekday,
                enabled=(
                    st.session_state["enabled"]
                    if "enabled" in st.session_state
                    else False
                ),
                last_modified=datetime.datetime.now(),
            )
        )
        return new_update

    def on_create_click_internal(self, *args) -> Optional[str]:
        # `current` is the upper-half of the schedule we're splitting, `row` is the lower-half.
        row, data, current, outcome = args
        if outcome is not None:
            return outcome

        # Create a new ID for the new row (current is the schedule we are splitting)
        new_row_id = row.id_val = uuid.uuid4().hex

        if current.name == "__empty__placeholder__" and current.size == "X-Small":
            # new bottom row
            data.append(row)
        else:
            # new middle row
            i = data.index(current)
            data.insert(i + 1, row)

        outcome, new_data = verify_and_clean(data)
        if outcome is not None:
            return outcome
        with connection.Connection.get() as conn, transaction(conn) as txn:
            try:
                # Check if verify_and_clean modified the new schedule
                new_row = next(i for i in new_data if i.id_val == new_row_id)
            except StopIteration:
                # The new schedule wasn't changed, write it as it originally was.
                new_row = row

            # Write the new schedule
            new_row.write(txn)

            # Make sure the rest of the rows are updated
            [i.update(txn, i) for i in new_data if i.id_val != new_row_id]
            # Twiddle the task state after adding a new schedule
            after_schedule_change(txn)

    def on_delete_click_internal(self, *args) -> Optional[str]:
        row = args[0][0]
        data = args[0][1]
        if len(data) == 1:
            return "Cannot delete the last schedule for a warehouse."
        index = data.index(row)
        del data[index]
        with connection.Connection.get() as conn, transaction(conn) as txn:
            row.delete(txn)
            comment, new_warehouses = verify_and_clean(data, ignore_errors=True)
            if comment is not None:
                return comment
            [i.update(txn, i) for i in new_warehouses]

            # Twiddle the task state after adding a new schedule
            after_schedule_change(txn)

    def on_update_click_internal(self, *args) -> Optional[str]:
        if args[3] is not None:
            return args[3]
        warehouses = args[1]
        i = warehouses.index(args[2])
        warehouses[i] = args[0]
        warehouses[i]._dirty = True
        comment, new_warehouses = verify_and_clean(warehouses)
        if comment is not None:
            return comment
        with connection.Connection.get() as conn, transaction(conn) as txn:
            [i.update(txn, i) for i in new_warehouses]
            # Twiddle the task state after a schedule has changed
            after_schedule_change(txn)

    def list(self):
        wh = st.session_state.get("warehouse")
        whfilter = wh.warehouse
        with connection.Connection.get() as conn:
            all_data, default_schedules = fetch_schedules_with_defaults(conn, whfilter)

        data = [i for i in all_data if i.weekday and i.name == whfilter]
        data_we = [i for i in all_data if not i.weekday and i.name == whfilter]

        is_enabled = all(i.enabled for i in data) and all(i.enabled for i in data_we)
        st.checkbox(
            "Enable Schedule",
            value=is_enabled,
            key="enabled",
            on_change=lambda: set_enabled(
                whfilter,
                (
                    st.session_state["enabled"]
                    if "enabled" in st.session_state
                    else False
                ),
            ),
        )

        # If the user has changed the schedules in any way and they are not enabled, give a hint to the
        # user that they may want to enable them.
        if not default_schedules and not is_enabled:
            st.info("Schedules are not running. Check the above box to enable them.")

        st.title("Weekdays")
        cbs = Actions(
            lambda x: self.session.do_edit(create_callback(data, x, weekday=True)),
            lambda row: self.on_delete_click(create_callback(data, row, weekday=True)),
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
            lambda row: self.on_delete_click(
                create_callback(data_we, row, weekday=False)
            ),
            lambda row: self.session.do_create(
                create_callback(data_we, row, weekday=False)
            ),
        )
        build_table(self.base_cls, data_we, cbs, has_empty=True)
