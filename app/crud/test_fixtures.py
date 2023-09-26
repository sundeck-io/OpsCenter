import datetime
import pandas as pd
from typing import List
import uuid
from snowflake.snowpark.exceptions import SnowparkSQLException
from .wh_sched import WarehouseSchedules


class MockSession:
    """
    A fake snowflake.snowpark.Session object for unit tests.
    """

    def __init__(self):
        self._sql = []

    def sql(self, sql, **kwargs):
        self._sql.append(sql)
        return self

    def collect(self):
        # GROSS. Tricks the tests into passing the check that a label name doesn't conflict with a QUERY_HISTORY column.
        # but only trying to match the name check and not the condition check.
        if (
            self.sql
            and self._sql[-1].endswith(
                "from reporting.enriched_query_history where false"
            )
            and self._sql[-1].startswith('select "')
        ):
            raise SnowparkSQLException("invalid identifier to make tests pass")
        return self


class WarehouseScheduleFixture:
    task_log: List[dict]
    statements_executed: List[str]
    last_task_run: datetime.datetime
    now: datetime.datetime

    def __init__(self):
        # Make sure each test method gets a fresh fixture
        self.task_log = []
        self.statements_executed = []
        self.last_task_run = datetime.datetime.now() - datetime.timedelta(minutes=15)
        self.now = datetime.datetime.now()

    _schedules_columns = [
        "id_val",
        "name",
        "start_at",
        "finish_at",
        "size",
        "suspend_minutes",
        "resume",
        "scale_min",
        "scale_max",
        "warehouse_mode",
        "weekday",
        "enabled",
    ]
    _schedules = [
        # Small during weekday, non-work hours.
        {
            "id_val": uuid.uuid4().hex,
            "name": "COMPUTE_WH",
            "start_at": datetime.time(0, 0),
            "finish_at": datetime.time(9, 0),
            "size": "Small",
            "suspend_minutes": 1,
            "resume": True,
            "scale_min": 0,
            "scale_max": 0,
            "warehouse_mode": "Inherit",
            "weekday": True,
            "enabled": True,
        },
        # X-Large with auto_suspend=15mins during work hours
        {
            "id_val": uuid.uuid4().hex,
            "name": "COMPUTE_WH",
            "start_at": datetime.time(9, 0),
            "finish_at": datetime.time(17, 0),
            "size": "X-Large",
            "suspend_minutes": 15,
            "resume": True,
            "scale_min": 0,
            "scale_max": 0,
            "warehouse_mode": "Inherit",
            "weekday": True,
            "enabled": True,
        },
        {
            "id_val": uuid.uuid4().hex,
            "name": "COMPUTE_WH",
            "start_at": datetime.time(17, 0),
            "finish_at": datetime.time(23, 59),
            "size": "Small",
            "suspend_minutes": 15,
            "resume": True,
            "scale_min": 0,
            "scale_max": 0,
            "warehouse_mode": "Inherit",
            "weekday": True,
            "enabled": True,
        },
        # XSmall on the weekends
        {
            "id_val": uuid.uuid4().hex,
            "name": "COMPUTE_WH",
            "start_at": datetime.time(0, 0),
            "finish_at": datetime.time(23, 59),
            "size": "X-Small",
            "suspend_minutes": 1,
            "resume": True,
            "scale_min": 0,
            "scale_max": 0,
            "warehouse_mode": "Inherit",
            "weekday": False,
            "enabled": True,
        },
    ]

    _warehouses = {
        "COMPUTE_WH": WarehouseSchedules(
            name="COMPUTE_WH",
            size="X-Small",
            suspend_minutes=1,
            resume=True,
            scale_min=0,
            scale_max=0,
            warehouse_mode="Standard",
        ),
        "BATCH_WH": WarehouseSchedules(
            name="BATCH_WH",
            size="Medium",
            suspend_minutes=5,
            resume=True,
            scale_min=0,
            scale_max=0,
            warehouse_mode="Standard",
        ),
        "AUTOSCALE_WH": WarehouseSchedules(
            name="AUTOSCALE_WH",
            size="Small",
            suspend_minutes=10,
            resume=True,
            scale_min=1,
            scale_max=3,
            warehouse_mode="Economy",
        ),
    }

    def get_schedules(self, is_weekday: bool) -> pd.DataFrame:
        df = pd.DataFrame.from_records(self._schedules, columns=self._schedules_columns)
        df.rename(
            columns={col_name: col_name.upper() for col_name in df.axes[1]},
            inplace=True,
        )
        return df[df["WEEKDAY"] == is_weekday]

    def override_warehouse_state(self, wh_name: str, state: WarehouseSchedules):
        """
        Alter the current state of the warehouse
        :param wh_name:
        :param state:
        :return:
        """
        self._warehouses[wh_name.upper()] = state

    def describe_warehouse(self, name: str) -> WarehouseSchedules:
        # Intentionally throw a KeyError if the warehouse with that name doesn't exist
        return self._warehouses[name]

    def log_task_run(self, task_start: datetime.datetime, success: bool, output):
        self.task_log.append(
            {
                "run": task_start,
                "success": success,
                "output": output,
            }
        )

    def run_statement(self, stmt: str):
        self.statements_executed.append(stmt)
