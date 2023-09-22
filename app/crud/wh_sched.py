import uuid
from typing import Optional, ClassVar
import datetime
from .base import BaseOpsCenterModel
from pydantic import validator, root_validator, Field
from snowflake.snowpark.functions import col, max as sp_max
from snowflake.snowpark import Row


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
    id_val: str = Field(default_factory=lambda: uuid.uuid4().hex)
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
    _dirty: bool = False

    class Config:
        underscore_attrs_are_private = True
        # allow_mutation = False

    def get_id_col(self) -> str:
        return "id_val"

    def get_id(self) -> str:
        return self.id_val

    @validator("name", allow_reuse=True)
    def verify_name(cls, v):
        if not v:
            raise ValueError("Name is required")
        assert isinstance(v, str)
        return v

    @validator("start_at", "finish_at", allow_reuse=True)
    def verify_time(cls, v):
        if v is None:
            raise ValueError("Time is required")
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
        assert v in _WAREHOUSE_SIZE_OPTIONS.keys()
        return v

    @validator("suspend_minutes", allow_reuse=True)
    def verify_suspend_minutes(cls, v):
        if v is None:
            raise ValueError("Suspend minutes is required")
        assert isinstance(v, int)
        assert v >= 0
        return v

    @validator("resume", "weekday", "enabled", allow_reuse=True)
    def verify_resume(cls, v):
        if v is None:
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
        start_at = values.get("start_at", datetime.time.min)
        finish_at = values.get(
            "finish_at", datetime.time.max.replace(microsecond=0, second=0)
        )
        if start_at and finish_at and start_at >= finish_at:
            raise ValueError("Start time must be before finish time")
        return values

    @root_validator(allow_reuse=True)
    @classmethod
    def verify_scales(cls, values):
        scale_min = values.get("scale_min", 0)
        scale_max = values.get("scale_max", 0)
        if scale_min >= 0 and scale_max >= 0 and scale_min > scale_max:
            raise ValueError("Scale min must be less than scale max")
        return values


def describe_warehouse(session, warehouse):
    wh_df = session.sql(f"show warehouses like '{warehouse}'").collect()
    wh_dict = wh_df[0].as_dict()
    return WarehouseSchedules(
        name=warehouse,
        size=wh_dict["size"],
        suspend_minutes=int(wh_dict["auto_suspend"] or 0) // 60,
        resume=wh_dict["auto_resume"],
        scale_min=wh_dict.get("min_cluster_count", 0),
        scale_max=wh_dict.get("max_cluster_count", 0),
        warehouse_mode=wh_dict.get("scaling_policy", "Standard"),
    )


def compare_warehouses(warehouse_now, warehouse_next):
    changes = []
    if (
        warehouse_now.scale_min == 0
        or warehouse_next.scale_min == 0
        or warehouse_now.scale_min == 0
        or warehouse_next.scale_min == 0
    ):
        is_standard = True
    else:
        is_standard = False
    if warehouse_now.size != warehouse_next.size:
        changes.append(
            f"WAREHOUSE_SIZE = {_WAREHOUSE_SIZE_OPTIONS[warehouse_next.size]}"
        )
        if "Snowpark" in warehouse_next.size and "Snowpark" not in warehouse_now.size:
            changes.append("WAREHOUSE_TYPE = SNOWPARK-OPTIMIZED")
        if "Snowpark" not in warehouse_next.size and "Snowpark" in warehouse_now.size:
            changes.append("WAREHOUSE_TYPE = STANDARD")
    if warehouse_now.suspend_minutes != warehouse_next.suspend_minutes:
        changes.append(f"AUTO_SUSPEND = {warehouse_next.suspend_minutes}")
    if warehouse_now.resume != warehouse_next.resume:
        changes.append(f"AUTO_RESUME = {warehouse_next.resume}")
    if warehouse_now.scale_min != warehouse_next.scale_min and not is_standard:
        changes.append(f"MIN_CLUSTER_COUNT = {warehouse_next.scale_min}")
    if warehouse_now.scale_max != warehouse_next.scale_max and not is_standard:
        changes.append(f"MAX_CLUSTER_COUNT = {warehouse_next.scale_max}")
    if (
        warehouse_now.warehouse_mode != warehouse_next.warehouse_mode
        and not is_standard
    ):
        changes.append(f"SCALING_POLICY = {warehouse_next.warehouse_mode}")
    return ", ".join(changes)


def update_warehouse(warehouse_now, warehouse_next):
    set_stmts = compare_warehouses(warehouse_now, warehouse_next)
    if set_stmts:
        return f"alter warehouse {warehouse_next.name} set {set_stmts}"
    return ""


def build_task_table(session, this_run):
    today = this_run.date()
    # TODO handle looking back over a weekend boundary
    # TODO handle the timestamp from config
    is_weekday = this_run.weekday() < 5

    last_run = (
        session.table("internal.task_warehouse_schedule")
        .filter(col("success") is True)
        .select(sp_max("run").as_("x"))
        .collect()[0]
        .X
    )
    if last_run is None:
        last_run = this_run - datetime.timedelta(days=1)
    scheds = (
        session.table("internal.wh_schedules")
        .filter(col("weekday") == is_weekday)
        .filter(col("enabled") is True)
        .to_pandas()
    )
    if scheds.empty:
        return scheds
    scheds["ts"] = scheds.START_AT.map(lambda x: build_ts(today, this_run, x))
    scheds["should_run"] = scheds.ts.map(lambda x: last_run <= x <= this_run)
    to_run = scheds[scheds.should_run].sort_values(by="ts").groupby("NAME").first()

    return to_run.reset_index()


def build_ts(today, now, x):
    if x < now.time():
        return datetime.datetime.combine(today, x).replace(tzinfo=now.tzinfo)
    else:
        return datetime.datetime.combine(today - datetime.timedelta(days=1), x).replace(
            tzinfo=now.tzinfo
        )


def build_statement(session, data):
    df = data
    df.columns = [c.lower() for c in df.columns]
    arr = [WarehouseSchedules(**dict(i)) for i in df.to_dict(orient="records")]
    allstmt = []
    wh_updated = []
    for wh in arr:
        wh_now = describe_warehouse(session, wh.name)
        stmt = update_warehouse(wh_now, wh)
        if stmt:
            allstmt.append(stmt)
            wh_updated.append(wh.name)
    if len(allstmt) == 0:
        return "", wh_updated
    joined_stmts = ";\n".join(allstmt)
    return (
        f"""begin
    {joined_stmts};
    end;""",
        wh_updated,
    )


def warehouse_schedule_task(session):
    this_run = session.sql("select current_timestamp as x").collect()[0].X
    success = False
    obj = dict()
    try:
        df = build_task_table(session, this_run)
        obj["candidates"] = len(df)
        stmt, wh_updated = build_statement(session, df)
        obj["wh_updated"] = wh_updated
        obj["stmt"] = stmt
        obj["update_count"] = len(wh_updated)
        if stmt:
            session.sql(stmt).collect()
        success = True
        return stmt
    except Exception as e:
        obj["error"] = str(e)
        raise e
    finally:
        log_to_table(session, this_run, success, obj)


def log_to_table(session, this_run, success, obj):
    df = session.createDataFrame(
        [
            Row(
                **{
                    "run": this_run.replace(tzinfo=None),
                    "success": success,
                    "output": obj,
                }
            )
        ]
    )
    df.write.mode("append").save_as_table("internal.task_warehouse_schedule")


_WAREHOUSE_SIZE_OPTIONS = {
    "X-Small": "XSMALL",
    "Small": "SMALL",
    "Medium": "MEDIUM",
    "Large": "LARGE",
    "X-Large": "XLARGE",
    "2X-Large": "XXLARGE",
    "3X-Large": "XXXLARGE",
    "4X-Large": "X4LARGE",
    "5X-Large": "X5LARGE",
    "6X-Large": "X6LARGE",
    "Medium Snowpark": "MEDIUM",
    "Large Snowpark": "LARGE",
    "X-Large Snowpark": "XLARGE",
    "2X-Large Snowpark": "XXLARGE",
    "3X-Large Snowpark": "XXXLARGE",
    "4X-Large Snowpark": "X4LARGE",
}

_WAREHOUSE_MODE_OPTIONS = [
    "Standard",
    "Economy",
    "Inherit",
]
