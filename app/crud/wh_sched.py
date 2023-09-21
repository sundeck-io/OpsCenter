from typing import Optional, ClassVar
import datetime
from .base import BaseOpsCenterModel
from pydantic import (
    validator,
    root_validator,
)


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

    @validator("name", allow_reuse=True)
    def verify_name(cls, v):
        if not v:
            raise ValueError("Name is required")
        assert isinstance(v, str)
        return v

    @validator("start_at", "finish_at", allow_reuse=True)
    def verify_time(cls, v):
        assert isinstance(v, datetime.time)
        assert (
            datetime.time.min <= v <= datetime.time.max.replace(microsecond=0, second=0)
        )
        return v

    @validator("size", allow_reuse=True)
    def verify_size(cls, v):
        if not v:
            raise ValueError("Size is required")
        assert isinstance(v, str)
        assert v in _WAREHOUSE_SIZE_OPTIONS
        return v

    @validator("suspend_minutes", allow_reuse=True)
    def verify_suspend_minutes(cls, v):
        if not v:
            raise ValueError("Suspend minutes is required")
        assert isinstance(v, int)
        assert v >= 0
        return v

    @validator("resume", "weekday", "enabled", allow_reuse=True)
    def verify_resume(cls, v):
        if not v:
            raise ValueError("Resume is required")
        assert isinstance(v, bool)
        return v

    @validator("scale_min", "scale_max", allow_reuse=True)
    def verify_scale(cls, v):
        if v is None:
            raise ValueError("Scale is required")
        assert isinstance(v, int)
        assert 10 >= v >= 0
        return v

    @validator("warehouse_mode", allow_reuse=True)
    def verify_warehouse_mode(cls, v):
        if not v:
            raise ValueError("Warehouse mode is required")
        assert isinstance(v, str)
        assert v in _WAREHOUSE_MODE_OPTIONS
        return v

    @root_validator(allow_reuse=True)
    @classmethod
    def verify_start_finish(cls, values):
        start_at = values.get("start_at")
        finish_at = values.get("finish_at")
        if start_at and finish_at and start_at >= finish_at:
            raise ValueError("Start time must be before finish time")
        return values

    @root_validator(allow_reuse=True)
    @classmethod
    def verify_scales(cls, values):
        scale_min = values.get("scale_min")
        scale_max = values.get("scale_max")
        if scale_min >= 0 and scale_max >= 0 and scale_min > scale_max:
            raise ValueError("Scale min must be less than scale max")
        return values


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

_WAREHOUSE_MODE_OPTIONS = [
    "Standard",
    "Economy",
    "Inherit",
]
