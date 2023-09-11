from typing import Optional, ClassVar
import datetime
from base import BaseOpsCenterModel


def format_suspend_minutes(value):
    if value == 0:
        return ""
    elif value == 1:
        return "1 minute"
    else:
        return f"{value} minutes"


## Adjust sizes
class WarehouseSchedules(BaseOpsCenterModel):
    table_name: ClassVar[str] = "WH_SCHEDULES"
    col_widths: ClassVar[dict] = {
        "start_at": ("Start", 1),
        "finish_at": ("Finish", 1),
        "size": ("Size", 1),
        "suspend_minutes": ("Suspend", 1, format_suspend_minutes),
        "resume": ("Resume", 1),
        "scale_min": ("Scale Min", 1, lambda x: max(x, 1)),
        "scale_max": ("Scale Max", 1, lambda x: max(x, 1)),
        "warehouse_mode": ("Mode", 1),
        "comment": ("Comment", 1),
    }
    name: str
    start_at: datetime.time = datetime.time.min
    finish_at: datetime.time = datetime.time.max.replace(microsecond=0, second=0)
    size: str
    suspend_minutes: int
    resume: bool
    scale_min: int
    scale_max: int
    warehouse_mode: str
    comment: Optional[str] = None
    weekday: bool = True
    day: Optional[str] = None
    enabled: bool = False

    def get_id_col(self) -> str:
        return "name"

    def get_id(self) -> str:
        return self.name
