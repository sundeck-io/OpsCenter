import uuid
from typing import ClassVar, List, Optional, Tuple
import datetime
from .base import BaseOpsCenterModel
from .errors import summarize_error
from pydantic import validator, root_validator, Field
from pytz import timezone
from pytz.exceptions import UnknownTimeZoneError
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
    max_cluster_size: ClassVar[int] = 10

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

    def autoscaling_enabled(self) -> bool:
        """
        If autoscaling is enabled on this warehouse.
        """
        # Sanity check that the instance is valid.
        assert (self.scale_min == 0 and self.scale_max == 0) or (
            self.scale_min > 0 and self.scale_max > 0
        )
        return self.scale_min > 0

    def st_min_cluster_value(self) -> int:
        """
        For the "value" attribute of "Min Clusters" on the streamlit.number_input.
        """
        return self.scale_min if self.autoscaling_enabled() else 0

    def st_min_cluster_minvalue(self) -> int:
        """
        For the "min_value" attribute of "Min Clusters" on the streamlit.number_input.
        """
        return 1 if self.autoscaling_enabled() else 0

    def st_min_cluster_maxvalue(self) -> int:
        """
        For the "max_value" attribute of "Min Clusters" on the streamlit.number_input.
        """
        return WarehouseSchedules.max_cluster_size if self.autoscaling_enabled() else 0

    def st_max_cluster_value(self) -> int:
        """
        For the "value" attribute of "Max Clusters" on the streamlit.number_input.
        """
        return self.scale_max if self.autoscaling_enabled() else 0

    def st_max_cluster_maxvalue(self):
        return WarehouseSchedules.max_cluster_size if self.autoscaling_enabled() else 0

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
        assert v.minute % 15 == 0 or v == datetime.time(
            23, 59
        ), "start_at and finish_at times must be on a 15-minute boundary"
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
        assert cls.max_cluster_size >= v >= 0
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

    @classmethod
    def find_all(cls, session: Session, name: str) -> List["WarehouseSchedules"]:
        """
        Syntactic sugar to return all schedules for a given warehouse, including both weekday
        and weekend schedules.
        """
        return cls.batch_read(
            session,
            sortby="start_at",
            filter=lambda df: (df.name == name),
        )

    @classmethod
    def find_all_with_weekday(
        cls, session: Session, name: str, weekday: bool
    ) -> List["WarehouseSchedules"]:
        """
        Syntactic sugar to return all schedules for a given warehouse and weekday/weekend.
        """
        return cls.batch_read(
            session,
            sortby="start_at",
            filter=lambda df: ((df.name == name) & (df.weekday == weekday)),
        )

    @classmethod
    def find_one(
        cls,
        session: Session,
        name: str,
        start_at: datetime.time,
        finish_at: datetime.time,
        weekday: bool,
    ) -> Optional["WarehouseSchedules"]:
        """
        Returns the schedule from a warehouse matching the start_at, finish_at, and weekday
        values. If no such schedule is found, this method returns `None`. If multiple schedules
        are found, it raises a ValueError.
        """
        rows = cls.batch_read(
            session,
            filter=lambda df: (
                (df.name == name)
                & (df.weekday == weekday)
                & (df.start_at == start_at)
                & (df.finish_at == finish_at)
            ),
        )
        if len(rows) == 0:
            return None
        if len(rows) > 1:
            raise ValueError(
                f"Found multiple schedules for {name} {start_at} {finish_at}, {'weekday' if weekday else 'weekend'}"
            )
        return rows[0]

    @classmethod
    def enable_scheduling(cls, session: Session, name: str, enabled: bool):
        """
        Enable or disable scheduling for the given warehouse.
        """
        _ = session.sql(
            f"update internal.{cls.table_name} set enabled = ? where name = ?",
            params=[enabled, name],
        ).collect()
        after_schedule_change(session)


class WarehouseAlterStatements(BaseOpsCenterModel):
    """
    Stores the ALTER WAREHOUSE statements for a given schedule.
    """

    table_name: ClassVar[str] = "WAREHOUSE_ALTER_STATEMENTS"
    id_val: str
    alter_statement: str

    def get_id_col(self) -> str:
        return "id_val"

    def get_id(self) -> str:
        return self.id_val

    @validator("id_val", allow_reuse=True)
    def verify_id(cls, v):
        if not v:
            raise ValueError("ID is required")
        assert isinstance(v, str)
        return v

    @validator("alter_statement", allow_reuse=True)
    def verify_alter_statement(cls, v):
        if not v:
            raise ValueError("The alter statement SQL is required")
        assert isinstance(v, str)
        return v

    @classmethod
    def create_table(cls, session, with_catalog_views=False):
        # Never create a catalog view for this table
        super(WarehouseAlterStatements, cls).create_table(session, False)


def convert_time_str(time_str) -> datetime.time:
    return datetime.datetime.strptime(time_str, "%I:%M %p").time()


def fetch_schedules_with_defaults(
    session: Session, warehouse: str
) -> List[WarehouseSchedules]:
    """
    Returns the WarehouseSchedules for the given warehouse.
    :param session: Snowpark session instance.
    :param warehouse: The snowflake warehouse to filter on.
    :return: A list of WarehouseSchedules for the given warehouse, creating and persisting
    default schedules if none exist.
    """
    schedules = WarehouseSchedules.find_all(session, warehouse)
    if len(schedules) == 0:
        wh = describe_warehouse(session, warehouse)
        wh.write(session)
        schedules.append(wh)
        wh2 = describe_warehouse(session, warehouse)
        wh2.weekday = False
        wh2.write(session)
        schedules.append(wh2)
    elif len(schedules) == 2 and all(not s.enabled for s in schedules):
        # If we have only 2 schedules which are both disabled, refresh the warehouse. `show warehouses` doesn't
        # use a warehouse, so we're probably OK doing this on every page-load.
        wh = describe_warehouse(session, warehouse)
        updated_schedules = []
        for s in schedules:
            # Copy the object so the two iterations of this loop don't use the same object (avoid 2 queries)
            update = WarehouseSchedules(**wh.dict())
            # Preserve the weekday flag and id_val
            update.weekday = s.weekday
            update.id_val = s.id_val
            updated_schedules.append(s.update(session, update))
        schedules = updated_schedules

    return schedules


def delete_warehouse_schedule(
    to_delete: WarehouseSchedules, schedules: List[WarehouseSchedules]
) -> List[WarehouseSchedules]:
    # Collapse any gaps in the schedule left by the delete
    if len(schedules) == 1:
        raise ValueError("Cannot delete the last schedule for a warehouse.")

    new_schedules = [s for s in schedules if s.id_val != to_delete.id_val]
    err_msg, new_schedules = verify_and_clean(new_schedules, ignore_errors=True)
    if err_msg:
        raise ValueError(err_msg)
    return new_schedules


def merge_new_schedule(
    new_schedule: WarehouseSchedules, existing_schedules: List[WarehouseSchedules]
) -> List[WarehouseSchedules]:
    """
    Validates and inserts the new schedule into the given list. Required for the SQL procedures because the UI is
    doing validation on its own through the form.
    :param new_schedule: User-provided schedule
    :param existing_schedules:  Existing schedules for this warehouse and weekday/weekend.
    :return:
    """
    if len(existing_schedules) == 0:
        if new_schedule.start_at != datetime.time(
            0, 0
        ) or new_schedule.finish_at != datetime.time(23, 59):
            raise ValueError(
                "First schedule for warehouse must start at 00:00 and end at 23:59."
            )
        return [new_schedule]

    # Assumes we have the default schedule encompassing the day.
    for i in range(0, len(existing_schedules)):
        # Find the schedule to insert the new schedule before
        if existing_schedules[i].finish_at == new_schedule.finish_at:
            new_schedule._dirty = True
            if i < len(existing_schedules) - 1:
                # If it's not the first schedule, set the previous schedule's finish_at to the new schedule's start_at
                existing_schedules.insert(i + 1, new_schedule)
                existing_schedules[i].finish_at = new_schedule.start_at
                existing_schedules[i]._dirty = True
            else:
                # Update the last schedule to point at the new schedule, and then add the new schedule to the end
                existing_schedules[-1].finish_at = new_schedule.start_at
                existing_schedules[-1]._dirty = True
                existing_schedules.append(new_schedule)
            break
    else:
        raise ValueError(
            f"Schedule must match an existing schedule's finish_at, got {new_schedule.finish_at}"
        )

    return existing_schedules


def update_existing_schedule(
    id_val: str, new_schedule: WarehouseSchedules, schedules: List[WarehouseSchedules]
) -> List[WarehouseSchedules]:
    """
    Updates the warehouse schedule identified by `id_val` to be `new_schedule`, and returns any WarehouseSchedules that
    need to be persisted to the table.
    :param id_val:  The ID of the warehouse schedule to update.
    :param new_schedule:  The updated warehouse schedule.
    :param schedules: Current schedules for this warehouse and weekday/weekend.
    :return:  The schedules that need updating.
    """
    wh_name = new_schedule.name

    # Replace the old schedule with the new one
    schedules = [new_schedule if i.id_val == id_val else i for i in schedules]

    # Make sure the new schedule is marked as dirty so it is returned by verify_and_clean
    new_schedule._dirty = True

    # Make sure the new schedule is sane
    err_msg, schedules_needing_update = verify_and_clean(schedules)
    if err_msg is not None:
        raise Exception(f"Failed to update schedule for {wh_name}, {err_msg}")

    return schedules_needing_update


def verify_and_clean(
    data: List[WarehouseSchedules], ignore_errors=False
) -> Tuple[Optional[str], List[WarehouseSchedules]]:
    """
    Verification and cleaning of all the WarehouseSchedules for a specific warehouse. In the default mode, an error
    in the data is returned as the string in the first element of the returned Tuple. Errors can be ignored by setting
    the `ignore_errors` kwarg to `True`.
    :param data:  The list of warehouse schedules for a specific warehouse.
    :param ignore_errors:  Whether validation errors should be ignored, default=False.
    :return: An optional error message and the list of WarehouseSchedules which have been cleaned and need to be updated.
    """
    if data[0].start_at != datetime.time(0, 0):
        if ignore_errors:
            data[0].start_at = datetime.time(0, 0)
            data[0]._dirty = True
        else:
            return "First row must start at midnight.", data
    if data[-1].finish_at != datetime.time(23, 59):
        if ignore_errors:
            data[-1].finish_at = datetime.time(23, 59)
            data[-1]._dirty = True
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
    for i in data:
        try:
            i.validate(i.dict())
        except Exception as e:
            return summarize_error("Verify failed", e) + "\n" + str(i), data
    return None, [i for i in data if i._dirty]


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


def generate_alter_from_schedule(
    schedule: WarehouseSchedules,
) -> WarehouseAlterStatements:
    changes = []
    if schedule.scale_min == 0 or schedule.scale_max == 0:
        is_standard = True
    else:
        is_standard = False

    changes.append(f"WAREHOUSE_SIZE = {_WAREHOUSE_SIZE_OPTIONS[schedule.size]}")
    if "Snowpark" in schedule.size:
        changes.append("WAREHOUSE_TYPE = SNOWPARK-OPTIMIZED")
    else:
        changes.append("WAREHOUSE_TYPE = STANDARD")
    changes.append(f"AUTO_SUSPEND = {schedule.suspend_minutes}")
    changes.append(f"AUTO_RESUME = {schedule.resume}")
    if not is_standard:
        changes.append(f"MIN_CLUSTER_COUNT = {schedule.scale_min}")
        changes.append(f"MAX_CLUSTER_COUNT = {schedule.scale_max}")
        if not schedule.warehouse_mode == "Inherit":
            changes.append(f"SCALING_POLICY = {schedule.warehouse_mode}")

    return WarehouseAlterStatements(
        id_val=schedule.id_val,
        alter_statement=f"alter warehouse {schedule.name} set {', '.join(changes)}",
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


def after_schedule_change(session: Session) -> bool:
    """
    Takes the current collection of warehouse schedules, records the alter warehouse statement
    for each schedule, and appropriately schedules the tasks to run.
    :return: True if any task is scheduled to run (resumed), False otherwise.
    """
    schedules = WarehouseSchedules.batch_read(session, sortby="start_at")

    # Generate alter statements for each schedule
    regenerate_alter_statements(session, schedules)
    # Update the task's state
    task_started = update_task_state(session, schedules)

    return task_started


def regenerate_alter_statements(session: Session, schedules: List[WarehouseSchedules]):
    """
    Given a list of WarehouseSchedules, generate the ALTER WAREHOUSE statements and write them to the
    WAREHOUSE_ALTER_STATEMENTS table. Any rows in the WarehouseAlterStatements table which are not included in
    the `schedules` will be deleted (e.g. `dataframe.write.mode('overwrite')`)
    :param session: Snowpark session
    :param schedules: List of WarehouseSessions
    """
    # Take the Schedule and generate the WarehouseAlterStatements object which contains the ALTER WAREHOUSE stmt.
    alter_stmts = [generate_alter_from_schedule(schedule) for schedule in schedules]

    # Write all the statements to the table, overwriting any existing statements.
    WarehouseAlterStatements.batch_write(session, alter_stmts, overwrite=True)


def get_schedule_timezone(session: Session) -> timezone:
    """
    Fetch the 'default_timezone' from the internal.config table. If there is no timezone set or the
    timezone which is set fails to parse, this function will return the timezone for 'America/Los_Angeles'.
    """
    str_tz = (
        session.call("internal.get_config", "default_timezone") or "America/Los_Angeles"
    )
    try:
        return timezone(str_tz)
    except UnknownTimeZoneError:
        return timezone("America/Los_Angeles")


def update_task_state(
    session: Session, schedules: List[WarehouseSchedules], tz=None
) -> bool:
    # Make sure we have at least one enabled schedule.
    enabled_schedules = [sch for sch in schedules if sch.enabled]
    if len(enabled_schedules) == 0:
        disable_all_tasks(session)
        return False

    # Indirection for unit tests. Caller is not expected to provide a timezone.
    if not tz:
        tz = get_schedule_timezone(session)

    # Build the cron list for the enabled schedules
    alter_statements = []
    for offset in task_offsets:
        # For each "offset", generate multiple statements
        #   1. ALTER TASK ... SUSPEND
        #   2. ALTER TASK ... SET SCHEDULE = 'USING CRON ...'
        #   3. ALTER TASK ... RESUME
        alter_statements.extend(
            _make_alter_task_statements(enabled_schedules, offset, tz)
        )

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
    schedules: List[WarehouseSchedules],
    offset: int,
    tz: timezone,
) -> List[str]:
    """
    Generates a list of ALTER TASK statements given the list of WarehouseSchedules and the task offset (the quarterly
    minute offset from the hour).
    """
    cron_schedule, should_run = _make_cron_schedule(schedules, offset, tz)
    if should_run:
        return [
            f"alter task if exists {task_name}_{offset} suspend;",
            f"alter task if exists {task_name}_{offset} set schedule = 'using cron {cron_schedule}';",
            f"alter task if exists {task_name}_{offset} resume;",
        ]
    else:
        return [f"alter task if exists {task_name}_{offset} suspend;"]


def _make_cron_schedule(
    schedules: List[WarehouseSchedules],
    offset: int,
    tz: timezone,
) -> (str, bool):
    """
    Takes a list of schedules and returns the cron schedule string which cover all schedule boundaries.
    :param schedules: A list of enabled WarehouseSchedules.
    """
    assert offset in task_offsets

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

    return f"{offset} {cron_hours} * * {days_of_week} {tz.zone}", True


def get_last_run(session: Session) -> datetime.datetime:
    return (
        session.table("internal.task_warehouse_schedule")
        .filter(col("success"))
        .select(sp_max("run").as_("x"))
        .collect()[0]
        .X
    )


def get_schedules(session: Session, is_weekday: bool) -> pd.DataFrame:
    return (
        session.table("internal.wh_schedules")
        .filter(col("weekday") == is_weekday)
        .filter(col("enabled"))
        .to_pandas()
    )


task_name: str = "TASKS.WAREHOUSE_SCHEDULING"
task_offsets: List[int] = [0, 15, 30, 45]


def disable_all_tasks(session: Session):
    statements = [
        f"alter task if exists {task_name}_{offset} suspend;" for offset in task_offsets
    ]
    statements.insert(0, "begin")
    statements.append("end;")
    session.sql("\n".join(statements)).collect()


def build_task_table(session: Session, this_run, last_run: datetime.datetime):
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
    scheds = get_schedules(session, is_weekday)
    if scheds.empty:
        return scheds

    # Convert a datetime from the start_time for this schedule, fixed on "today".
    scheds["ts"] = scheds.START_AT.map(lambda x: build_ts(today, this_run, x))
    # Then, determine which schedules should have run since the last time the task ran.
    scheds["should_run"] = scheds.ts.map(lambda x: last_run <= x <= this_run)
    # Finally, take the last schedule for each warehouse that should have run (to transparently handle
    # the task failing to run on the expected 15minute boundaries)
    to_run = scheds[scheds.should_run].sort_values(by="ts").groupby("NAME").last()

    return to_run.reset_index()


def build_ts(today, now, schedule_start_time: datetime.time):
    # localize the start_time for the schedule to today's date
    return datetime.datetime.combine(today, schedule_start_time).replace(
        tzinfo=now.tzinfo
    )


def build_statement(session: Session, data):
    df = data
    arr = WarehouseSchedules.from_df(df)
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


def now(session: Session):
    return session.sql("select current_timestamp as x").collect()[0].X


def run(session: Session):
    this_run = now(session)
    success = False
    obj = dict()
    try:
        last_run = get_last_run(session)

        df = build_task_table(session, this_run, last_run)
        obj["candidates"] = len(df)
        stmt, wh_updated = build_statement(session, df)
        obj["wh_updated"] = wh_updated
        obj["stmt"] = stmt
        obj["update_count"] = len(wh_updated)
        if stmt:
            run_statement(session, stmt)
        success = True
        return stmt
    except Exception as e:
        obj["error"] = str(e)
        raise e
    finally:
        log_to_table(session, this_run, success, obj)


def run_statement(session: Session, stmt: str):
    session.sql(stmt).collect()


def log_to_table(session: Session, this_run, success, obj):
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
