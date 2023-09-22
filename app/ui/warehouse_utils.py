import connection
from crud.wh_sched import WarehouseSchedules
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
    # WarehouseSchedules.create_table(connection.Connection.get())
    warehouses = WarehouseSchedules.batch_read(connection.Connection.get(), "start_at")
    if any(i for i in warehouses if i.name == warehouse) == 0:
        wh = describe_warehouse(warehouse)
        wh.write(connection.Connection.get())
        warehouses.append(wh)
        wh2 = describe_warehouse(warehouse)
        wh2.weekday = False
        wh2.write(connection.Connection.get())
        warehouses.append(wh2)
    return warehouses


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


def flip_enabled(wh_name: str):
    connection.execute(
        f"update internal.{WarehouseSchedules.table_name} set enabled = not enabled where name = '{wh_name}'"
    )


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
