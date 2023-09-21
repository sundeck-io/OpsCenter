import connection
from crud.wh_sched import WarehouseSchedules
from typing import Optional, List, Tuple
import datetime


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
