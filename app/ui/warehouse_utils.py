import connection
from crud.base import transaction
from crud.wh_sched import (
    WarehouseSchedules,
    after_schedule_change,
)
from typing import List
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


def convert_time_str(time_str) -> datetime.time:
    return datetime.datetime.strptime(time_str, "%I:%M %p").time()


def set_enabled(wh_name: str, enabled: bool):
    with connection.Connection.get() as conn, transaction(conn) as txn:
        _ = txn.sql(
            f"update internal.{WarehouseSchedules.table_name} set enabled = ? where name = ?",
            params=[enabled, wh_name],
        ).collect()
        after_schedule_change(txn)


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
