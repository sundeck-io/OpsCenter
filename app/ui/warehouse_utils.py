import connection
from snowflake.snowpark import Session
from crud.wh_sched import (
    WarehouseSchedules,
    describe_warehouse as crud_describe_warehouse,
    update_task_state as crud_update_task_state,
    regenerate_alter_statements as crud_regenerate_alter_statements,
)
from crud.errors import summarize_error
from typing import Optional, List, Tuple
import datetime


def create_callback(data, row, **additions):
    if row is None:
        row = WarehouseSchedules.construct(
            name="__empty__placeholder__",
            size="X-Small",
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
    with connection.Connection.get() as conn:
        warehouses = WarehouseSchedules.batch_read(conn, "start_at")
        if any(i for i in warehouses if i.name == warehouse) == 0:
            wh = crud_describe_warehouse(conn, warehouse)
            wh.write(conn)
            warehouses.append(wh)
            wh2 = crud_describe_warehouse(conn, warehouse)
            wh2.weekday = False
            wh2.write(conn)
            warehouses.append(wh2)
        return warehouses


def convert_time_str(time_str) -> datetime.time:
    return datetime.datetime.strptime(time_str, "%I:%M %p").time()


def verify_and_clean(
    data: List[WarehouseSchedules], ignore_errors=False
) -> Tuple[Optional[str], List[WarehouseSchedules]]:
    if data[0].start_at != datetime.time(0, 0):
        if ignore_errors:
            data[0].start_at = datetime.time(0, 0)
            data[0]._dirty = True
        else:
            return "First row must start at midnight.", data
    if data[-1].finish_at != datetime.time(23, 59):
        if ignore_errors:
            data[-1].finish_at = datetime.time(23, 59)
            data[0]._dirty = True
        else:
            return "Last row must end at midnight.", data
    next_start = data[0]
    for row in data[1:]:
        if row.start_at != next_start.finish_at:
            next_start.finish_at = row.start_at
            next_start._dirty = True
        if row.warehouse_mode == "Inherit":
            row.warehouse_mode = next_start.warehouse_mode
            row._dirty = True
        next_start = row
    try:
        [i.validate(i.dict()) for i in data]
    except Exception as e:
        return summarize_error("Verify failed", e), data
    return None, [i for i in data if i._dirty]


def set_enabled(wh_name: str, enabled: bool):
    with connection.Connection.get() as conn:
        # TODO I saw a case where one warehouse was in an indeterminate state for enabled (some false, some true).
        # Keep an eye on this. May have to push the explicitly bool value.
        _ = conn.sql(
            f"update internal.{WarehouseSchedules.table_name} set enabled = ? where name = ?",
            params=[enabled, wh_name],
        ).collect()
        after_schedule_change(conn)


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


def after_schedule_change(session: Session) -> bool:
    """
    Takes the current collection of warehouse schedules, records the alter warehouse statement
    for each schedule, and appropriately schedules the tasks to run.
    :return: True if any task is scheduled to run (resumed), False otherwise.
    """
    schedules = WarehouseSchedules.batch_read(session, sortby="start_at")

    # Generate alter statements for each schedule
    crud_regenerate_alter_statements(session, schedules)
    # Update the task's state
    task_started = crud_update_task_state(session, schedules)

    return task_started
