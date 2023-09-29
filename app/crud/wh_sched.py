import uuid
from typing import ClassVar, List, Optional
import datetime
from .base import BaseOpsCenterModel
from pydantic import validator, root_validator, Field
from snowflake.snowpark.functions import col, max as sp_max
from snowflake.snowpark import Row, Session
import pandas as pd


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

    @classmethod
    def create_table(cls, session, with_catalog_views=True):
        super(WarehouseSchedules, cls).create_table(session, with_catalog_views)
        # Create the task table for warehouse_schedules
        session.sql(
            "CREATE TABLE IF NOT EXISTS internal.task_warehouse_schedule("
            + "RUN TIMESTAMP_LTZ(9), SUCCESS BOOLEAN, OUTPUT VARIANT)"
        ).collect()


def describe_warehouse(session: Session, warehouse: str):
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


def compare_warehouses(
    warehouse_now: WarehouseSchedules, warehouse_next: WarehouseSchedules
):
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
        and not warehouse_next.warehouse_mode == "Inherit"
    ):
        changes.append(f"SCALING_POLICY = {warehouse_next.warehouse_mode}")
    return ", ".join(changes)


def update_warehouse(warehouse_now, warehouse_next):
    set_stmts = compare_warehouses(warehouse_now, warehouse_next)
    if set_stmts:
        return f"alter warehouse {warehouse_next.name} set {set_stmts}"
    return ""


def update_task_state(session: Session, schedules: List[WarehouseSchedules]) -> bool:
    # Make sure we have at least one enabled schedule.
    enabled_schedules = [sch for sch in schedules if sch.enabled]
    if len(enabled_schedules) == 0:
        WarehouseSchedulesTask.disable_all_tasks(session)
        return False

    # Build the cron list for the enabled schedules
    alter_statements = []
    for offset in WarehouseSchedulesTask.task_offsets:
        # For each "offset", generate two statements
        #   1. ALTER TASK ... SET SCHEDULE = 'USING CRON ...'
        #   2. ALTER TASK ... RESUME
        alter_statements.extend(_make_alter_task_statements(schedules, offset))

    # Collect the statements together
    alter_body = "\n".join(alter_statements)

    # Join all the statements together into one scripting block
    alter_task_sql = f"""begin
        {alter_body}
    end;"""

    # Run the whole block.
    session.sql(alter_task_sql).collect()
    return True


def _make_alter_task_statements(
    schedules: List[WarehouseSchedules], offset: int
) -> List[str]:
    """
    Generates a list of ALTER TASK statements given the list of WarehouseSchedules and the task offset (the quarterly
    minute offset from the hour).
    """
    cron_schedule, should_run = _make_cron_schedule(schedules, offset)
    if should_run:
        return [
            f"alter task if exists {WarehouseSchedulesTask.task_name}_{offset} suspend;",
            f"alter task if exists {WarehouseSchedulesTask.task_name}_{offset} set schedule = 'using cron {cron_schedule}';",
            f"alter task if exists {WarehouseSchedulesTask.task_name}_{offset} resume;",
        ]
    else:
        return [
            f"alter task if exists {WarehouseSchedulesTask.task_name}_{offset} suspend;"
        ]


def _make_cron_schedule(
    schedules: List[WarehouseSchedules], offset: int
) -> (str, bool):
    """
    Takes a list of schedules and returns the cron schedule string which cover all schedule boundaries.
    :param schedules: A list of enabled WarehouseSchedules.
    """
    assert offset in WarehouseSchedulesTask.task_offsets

    # Collect the schedules that start at the given offset "minutes"
    schedules_for_offset = [sch for sch in schedules if sch.start_at.minute == offset]

    if len(schedules_for_offset) > 0:
        # Collect all the unique hours that start at this offset
        all_hours = set([sch.start_at.hour for sch in schedules_for_offset])
        # Sort the hours and join them together
        cron_hours = ",".join([str(hr) for hr in sorted(all_hours)])
    else:
        return "", False

    weekdays_on_schedules = set([sch.weekday for sch in schedules_for_offset])
    # Using `weekday: bool` on the WarehouseSchedule, determine if we need to...
    if len(weekdays_on_schedules) > 1:
        # Execute weekdays and weekends
        days_of_week = "*"
    elif True in weekdays_on_schedules:
        # Execute only weekdays
        days_of_week = "1-5"
    else:
        # Execute only weekends
        days_of_week = "0,6"

    # TODO use the configured timezone
    return f"{offset} {cron_hours} * * {days_of_week} America/Los_Angeles", True


class WarehouseSchedulesTask:
    """
    WarehouseSchedulesTask encapsulates the logic to apply the warehouse schedules against the warehouses
    in the Snowflake Account. The logic in this class is stateless and does not need the encapsulation that the class
    provides; however, this class eases the injection of test data via unittest.mock.

    TODO move the logic in this class to functions in this file and figure out how to updated the mocking invocations to
         override the methods which execute queries against Snowflake.
    """

    task_name: ClassVar[str] = "TASKS.WAREHOUSE_SCHEDULING"
    task_offsets: ClassVar[List[int]] = [0, 15, 30, 45]

    @classmethod
    def disable_all_tasks(cls, session: Session):
        statements = [
            f"alter task if exists {cls.task_name}_{offset} suspend;"
            for offset in cls.task_offsets
        ]
        statements.insert(0, "begin")
        statements.append("end;")
        session.sql("\n".join(statements)).collect()

    session: Session = None

    def __init__(self, session: Session):
        self.session = session

    def describe_warehouse(self, wh_name: str):
        """
        Indirection around the static method describe_warehouse for testing. Even if
        we patch the static method, the task still refers to the unpatched method.
        """
        return describe_warehouse(self.session, wh_name)

    def get_last_run(self) -> datetime.datetime:
        return (
            self.session.table("internal.task_warehouse_schedule")
            .filter(col("success"))
            .select(sp_max("run").as_("x"))
            .collect()[0]
            .X
        )

    def get_schedules(self, is_weekday: bool) -> pd.DataFrame:
        return (
            self.session.table("internal.wh_schedules")
            .filter(col("weekday") == is_weekday)
            .filter(col("enabled"))
            .to_pandas()
        )

    def build_task_table(self, this_run, last_run: datetime.datetime):
        today = this_run.date()
        # TODO handle looking back over a weekend boundary
        # TODO handle the timestamp from config
        is_weekday = this_run.weekday() < 5

        # Look for time of last successful run.
        # If we have no successful past runs, claim the last run was from a day ago so we consider
        # all schedules that are scheduled for "today"
        if last_run is None:
            last_run = this_run - datetime.timedelta(days=1)

        # Find all schedules for weekend/weekday (whichever we're currently in)
        scheds = self.get_schedules(is_weekday)
        if scheds.empty:
            return scheds

        # Convert a datetime from the start_time for this schedule, fixed on "today".
        scheds["ts"] = scheds.START_AT.map(lambda x: self.build_ts(today, this_run, x))
        # Then, determine which schedules should have run since the last time the task ran.
        scheds["should_run"] = scheds.ts.map(lambda x: last_run <= x <= this_run)
        # Finally, take the last schedule for each warehouse that should have run (to transparently handle
        # the task failing to run on the expected 15minute boundaries)
        to_run = scheds[scheds.should_run].sort_values(by="ts").groupby("NAME").last()

        return to_run.reset_index()

    def build_ts(self, today, now, schedule_start_time: datetime.time):
        # localize the start_time for the schedule to today's date
        return datetime.datetime.combine(today, schedule_start_time).replace(
            tzinfo=now.tzinfo
        )

    def build_statement(self, data):
        df = data
        df.columns = [c.lower() for c in df.columns]
        arr = [WarehouseSchedules(**dict(i)) for i in df.to_dict(orient="records")]
        allstmt = []
        wh_updated = []
        for wh in arr:
            wh_now = self.describe_warehouse(wh.name)
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

    def now(self):
        return self.session.sql("select current_timestamp as x").collect()[0].X

    def run(self):
        this_run = self.now()
        success = False
        obj = dict()
        try:
            last_run = self.get_last_run()

            df = self.build_task_table(this_run, last_run)
            obj["candidates"] = len(df)
            stmt, wh_updated = self.build_statement(df)
            obj["wh_updated"] = wh_updated
            obj["stmt"] = stmt
            obj["update_count"] = len(wh_updated)
            if stmt:
                self.run_statement(stmt)
            success = True
            return stmt
        except Exception as e:
            obj["error"] = str(e)
            raise e
        finally:
            self.log_to_table(this_run, success, obj)

    def run_statement(self, stmt: str):
        self.session.sql(stmt).collect()

    def log_to_table(self, this_run, success, obj):
        df = self.session.createDataFrame(
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
