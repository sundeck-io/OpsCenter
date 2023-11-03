import datetime
from typing import List
from contextlib import contextmanager
import pandas as pd
from unittest.mock import patch
from . import wh_sched
from .test_fixtures import WarehouseScheduleFixture


@contextmanager
def _mock_task(session, fixtures: WarehouseScheduleFixture):
    with patch.object(wh_sched, "get_last_run") as mocked_get_last_run, patch.object(
        wh_sched, "now"
    ) as mocked_now, patch.object(
        wh_sched, "get_schedules"
    ) as mocked_get_schedules, patch.object(
        wh_sched, "describe_warehouse"
    ) as mock_describe_warehouse, patch.object(
        wh_sched, "log_to_table"
    ) as mock_log_to_table, patch.object(
        wh_sched, "run_statement"
    ) as mock_run_statement:
        # self.now()
        mocked_now.return_value = fixtures.now
        # self.last_task_run()
        mocked_get_last_run.return_value = fixtures.last_task_run
        # self.get_schedules(is_weekday)
        mocked_get_schedules.side_effect = (
            lambda *args, **kwargs: fixtures.get_schedules(args[1])
        )
        # self.describe_warehouse(str)
        mock_describe_warehouse.side_effect = (
            lambda *args, **kwargs: fixtures.describe_warehouse(args[1])
        )
        # self.log_to_table(this_run, success, obj)
        mock_log_to_table.side_effect = lambda *args, **kwargs: fixtures.log_task_run(
            args[1], args[2], args[3]
        )
        # self.run_statement(stmt)
        mock_run_statement.side_effect = lambda *args, **kwargs: fixtures.run_statement(
            args[1]
        )

        yield wh_sched


def test_basic_task(session, wh_sched_fixture):
    # Set the last time the task ran and "now" to a weekday that matches the test data.
    wh_sched_fixture.last_task_run = datetime.datetime.combine(
        datetime.date(2023, 9, 26), datetime.time(8, 45)
    )
    wh_sched_fixture.now = datetime.datetime.combine(
        datetime.date(2023, 9, 26), datetime.time(9, 0)
    )

    with _mock_task(session, wh_sched_fixture) as task:
        # Run the task
        alter_warehouse_block = task.run(session)

        # Verify the ALTER WAREHOUSE command was correct
        statements = _extract_alter_statements(
            alter_warehouse_block, warehouse="COMPUTE_WH"
        )
        assert len(statements) == 1
        assert "alter warehouse COMPUTE_WH set".lower() in statements[0].lower()
        assert "WAREHOUSE_SIZE = XLARGE".lower() in statements[0].lower()
        assert "AUTO_SUSPEND = 900".lower() in statements[0].lower()

        # Verify the task result was logged into the internal table
        assert len(wh_sched_fixture.task_log) == 1
        assert wh_sched_fixture.task_log[0].get("success"), "Task should have succeeded"


def test_noop_weekday_task(session, wh_sched_fixture):
    """
    Over the weekend, the task should do nothing if the warehouse doesn't need change.
    """
    wh_sched_fixture.schedule_filter = lambda df: df[df["NAME"] == "COMPUTE_WH"]

    # Set the last time the task ran and "now" to a weekend day that matches the test data.
    wh_sched_fixture.last_task_run = datetime.datetime.combine(
        datetime.date(2023, 9, 26), datetime.time(8, 0)
    )
    wh_sched_fixture.now = datetime.datetime.combine(
        datetime.date(2023, 9, 27), datetime.time(8, 15)
    )

    # Schedule expects Small during the week before 0900
    wh_sched_fixture.override_warehouse_state(
        "COMPUTE_WH",
        wh_sched.WarehouseSchedules(
            name="COMPUTE_WH",
            size="Small",
            suspend_minutes=1,
            resume=True,
            scale_min=0,
            scale_max=0,
            warehouse_mode="Standard",
        ),
    )

    with _mock_task(session, wh_sched_fixture) as task:
        # Run the task
        alter_warehouse_block = task.run(session)
        # The task should not have made any changes
        assert _no_task_action(alter_warehouse_block, wh_sched_fixture.task_log)


def test_noop_weekend_task(session, wh_sched_fixture):
    """
    Over the weekend, the task should do nothing if the warehouse doesn't need change.
    """
    # Set the last time the task ran and "now" to a weekend day that matches the test data.
    wh_sched_fixture.last_task_run = datetime.datetime.combine(
        datetime.date(2023, 9, 30), datetime.time(8, 45)
    )
    wh_sched_fixture.now = datetime.datetime.combine(
        datetime.date(2023, 9, 30), datetime.time(9, 0)
    )

    with _mock_task(session, wh_sched_fixture) as task:
        # Run the task
        alter_warehouse_block = task.run(session)
        # The task should not have made any changes
        assert _no_task_action(alter_warehouse_block, wh_sched_fixture.task_log)


def test_basic_weekend_task(session, wh_sched_fixture):
    """
    Make sure the task chooses the weekend schedule instead of the weekday task.
    """
    # First task run on the weekend.
    wh_sched_fixture.last_task_run = datetime.datetime.combine(
        datetime.date(2023, 9, 29), datetime.time(23, 45)
    )
    wh_sched_fixture.now = datetime.datetime.combine(
        datetime.date(2023, 9, 30), datetime.time(0, 0)
    )

    # Tweak the current state of COMPUTE_WH
    wh_sched_fixture.override_warehouse_state(
        "COMPUTE_WH",
        wh_sched.WarehouseSchedules(
            name="COMPUTE_WH",
            size="Medium",
            suspend_minutes=15,
            resume=True,
            scale_min=0,
            scale_max=0,
            warehouse_mode="Standard",
        ),
    )
    with _mock_task(session, wh_sched_fixture) as task:
        # Run the task
        alter_warehouse_block = task.run(session)

        # Verify the ALTER WAREHOUSE command was correct
        statements = _extract_alter_statements(alter_warehouse_block)
        assert len(statements) == 1
        assert "alter warehouse COMPUTE_WH set".lower() in statements[0].lower()
        assert "WAREHOUSE_SIZE = XSMALL".lower() in statements[0].lower()
        assert "AUTO_SUSPEND = 60".lower() in statements[0].lower()

        # Verify the task result was logged into the internal table
        assert len(wh_sched_fixture.task_log) == 1
        assert wh_sched_fixture.task_log[0].get("success"), "Task should have succeeded"


def test_basic_weekday_task(session, wh_sched_fixture):
    """
    Make sure the weekday schedule instead of the weekend task.
    """
    # First task run on the weekday.
    wh_sched_fixture.last_task_run = datetime.datetime.combine(
        datetime.date(2023, 9, 25), datetime.time(23, 45)
    )
    wh_sched_fixture.now = datetime.datetime.combine(
        datetime.date(2023, 9, 26), datetime.time(0, 0)
    )

    # Tweak the current state of COMPUTE_WH so the task definitely does something.
    wh_sched_fixture.override_warehouse_state(
        "COMPUTE_WH",
        wh_sched.WarehouseSchedules(
            name="COMPUTE_WH",
            size="Medium",
            suspend_minutes=15,
            resume=True,
            scale_min=0,
            scale_max=0,
            warehouse_mode="Standard",
        ),
    )
    with _mock_task(session, wh_sched_fixture) as task:
        # Run the task
        alter_warehouse_block = task.run(session)

        # Verify the ALTER WAREHOUSE command was correct
        statements = _extract_alter_statements(
            alter_warehouse_block, warehouse="COMPUTE_WH"
        )
        assert len(statements) == 1
        assert "alter warehouse COMPUTE_WH set".lower() in statements[0].lower()
        assert "WAREHOUSE_SIZE = SMALL".lower() in statements[0].lower()
        assert "AUTO_SUSPEND = 60".lower() in statements[0].lower()

        # Verify the task result was logged into the internal table
        assert len(wh_sched_fixture.task_log) == 1
        assert wh_sched_fixture.task_log[0].get("success"), "Task should have succeeded"


def test_multi_cluster_warehouse(session, wh_sched_fixture):
    """
    The task should correctly set scale_min/scale_max
    """

    # Filter the schedules to only the AUTOSCALE_WH schedules
    wh_sched_fixture.schedule_filter = lambda df: df[df["NAME"] == "AUTOSCALE_WH"]

    # Start at cluster size (1,2)
    wh_sched_fixture.last_task_run = datetime.datetime.combine(
        datetime.date(2023, 9, 26), datetime.time(8, 45)
    )
    wh_sched_fixture.now = datetime.datetime.combine(
        datetime.date(2023, 9, 26), datetime.time(9, 0)
    )
    with _mock_task(session, wh_sched_fixture) as task:
        # Run the task
        alter_warehouse_block = task.run(session)

        # Verify the ALTER WAREHOUSE command was correct
        statements = _extract_alter_statements(alter_warehouse_block)
        assert len(statements) == 1
        assert "alter warehouse AUTOSCALE_WH set".lower() in statements[0].lower()
        assert "MAX_CLUSTER_COUNT = 2".lower() in statements[0].lower()

        # Verify the task result was logged into the internal table
        assert len(wh_sched_fixture.task_log) == 1
        assert wh_sched_fixture.task_log[0].get("success"), "Task should have succeeded"

    # Then to (2,4)
    wh_sched_fixture.last_task_run = datetime.datetime.combine(
        datetime.date(2023, 9, 26), datetime.time(11, 45)
    )
    wh_sched_fixture.now = datetime.datetime.combine(
        datetime.date(2023, 9, 26), datetime.time(12, 0)
    )
    # Update warehouse to last state
    wh_sched_fixture.override_warehouse_state(
        "AUTOSCALE_WH",
        wh_sched.WarehouseSchedules(
            name="AUTOSCALE_WH",
            size="X-Small",
            suspend_minutes=1,
            resume=True,
            scale_min=1,
            scale_max=2,
            warehouse_mode="Standard",
        ),
    )
    with _mock_task(session, wh_sched_fixture) as task:
        # Run the task
        alter_warehouse_block = task.run(session)

        # Verify the ALTER WAREHOUSE command was correct
        statements = _extract_alter_statements(alter_warehouse_block)
        assert len(statements) == 1
        assert "alter warehouse AUTOSCALE_WH set".lower() in statements[0].lower()
        assert "MIN_CLUSTER_COUNT = 2".lower() in statements[0].lower()
        assert "MAX_CLUSTER_COUNT = 4".lower() in statements[0].lower()

        # Verify the task result was logged into the internal table
        assert len(wh_sched_fixture.task_log) == 2
        assert wh_sched_fixture.task_log[-1].get(
            "success"
        ), "Task should have succeeded"

    # The last window going back to (1,1)
    wh_sched_fixture.last_task_run = datetime.datetime.combine(
        datetime.date(2023, 9, 26), datetime.time(16, 45)
    )
    wh_sched_fixture.now = datetime.datetime.combine(
        datetime.date(2023, 9, 26), datetime.time(17, 0)
    )
    # Update warehouse to last state
    wh_sched_fixture.override_warehouse_state(
        "AUTOSCALE_WH",
        wh_sched.WarehouseSchedules(
            name="AUTOSCALE_WH",
            size="X-Small",
            suspend_minutes=1,
            resume=True,
            scale_min=2,
            scale_max=4,
            warehouse_mode="Standard",
        ),
    )
    with _mock_task(session, wh_sched_fixture) as task:
        # Run the task
        alter_warehouse_block = task.run(session)

        # Verify the ALTER WAREHOUSE command was correct
        statements = _extract_alter_statements(alter_warehouse_block)
        assert len(statements) == 1
        assert "alter warehouse AUTOSCALE_WH set".lower() in statements[0].lower()
        assert "MIN_CLUSTER_COUNT = 1".lower() in statements[0].lower()
        assert "MAX_CLUSTER_COUNT = 1".lower() in statements[0].lower()

        # Verify the task result was logged into the internal table
        assert len(wh_sched_fixture.task_log) == 3
        assert wh_sched_fixture.task_log[-1].get(
            "success"
        ), "Task should have succeeded"


def test_missed_task_execution(session, wh_sched_fixture):
    """
    The task should correctly run the most-recent schedule if it hasn't run for some time.
    """

    # Filter the schedules to only the COMPUTE_WH schedules
    wh_sched_fixture.schedule_filter = lambda df: df[df["NAME"] == "COMPUTE_WH"]

    # A weekday, but a big gap since last run
    wh_sched_fixture.last_task_run = datetime.datetime.combine(
        datetime.date(2023, 9, 25), datetime.time(23, 45)
    )
    wh_sched_fixture.now = datetime.datetime.combine(
        datetime.date(2023, 9, 26), datetime.time(18, 30)
    )

    # Override the COMPUTE_WH current state
    wh_sched_fixture.override_warehouse_state(
        "COMPUTE_WH",
        wh_sched.WarehouseSchedules(
            name="COMPUTE_WH",
            size="Medium",
            suspend_minutes=10,
            resume=True,
            scale_min=0,
            scale_max=0,
            warehouse_mode="Standard",
        ),
    )
    with _mock_task(session, wh_sched_fixture) as task:
        # Run the task
        alter_warehouse_block = task.run(session)

        # Verify the ALTER WAREHOUSE command was correct
        statements = _extract_alter_statements(alter_warehouse_block)
        assert len(statements) == 1
        assert "alter warehouse COMPUTE_WH set".lower() in statements[0].lower()
        assert "WAREHOUSE_SIZE = SMALL".lower() in statements[0].lower()
        assert "AUTO_SUSPEND = 900".lower() in statements[0].lower()

        # Verify the task result was logged into the internal table
        assert len(wh_sched_fixture.task_log) == 1
        assert wh_sched_fixture.task_log[0].get("success"), "Task should have succeeded"


def test_disabled_schedules_do_nothing(session, wh_sched_fixture):
    """
    The disabled schedules should not trigger warehouse changes
    """

    # Mark all schedules as disabled
    wh_sched_fixture.schedule_filter = lambda df: df.assign(ENABLED=False)
    # Set the last time the task ran and "now" to a weekend day that matches the test data.
    wh_sched_fixture.last_task_run = datetime.datetime.combine(
        datetime.date(2023, 9, 26), datetime.time(8, 45)
    )
    wh_sched_fixture.now = datetime.datetime.combine(
        datetime.date(2023, 9, 27), datetime.time(9, 00)
    )

    # Change the current COMPUTE_WH so the schedule should make a change if it were enabled
    wh_sched_fixture.override_warehouse_state(
        "COMPUTE_WH",
        wh_sched.WarehouseSchedules(
            name="COMPUTE_WH",
            size="2X-Large",
            suspend_minutes=1,
            resume=True,
            scale_min=0,
            scale_max=0,
            warehouse_mode="Standard",
        ),
    )

    with _mock_task(session, wh_sched_fixture) as task:
        # Run the task
        alter_warehouse_block = task.run(session)
        # The task should not have made any changes since we disabled all schedules
        assert _no_task_action(alter_warehouse_block, wh_sched_fixture.task_log)


def test_inherit_scaling_policy(session, wh_sched_fixture):
    """
    Inherit should not change the scaling policy (default in the fixtures)
    """
    # Filter the schedules to only the AUTOSCALE_WH schedules
    wh_sched_fixture.schedule_filter = lambda df: df[df["NAME"] == "AUTOSCALE_WH"]

    # Cluster should be set to (1,2)
    wh_sched_fixture.last_task_run = datetime.datetime.combine(
        datetime.date(2023, 9, 26), datetime.time(8, 45)
    )
    wh_sched_fixture.now = datetime.datetime.combine(
        datetime.date(2023, 9, 26), datetime.time(9, 0)
    )

    wh_sched_fixture.override_warehouse_state(
        "AUTOSCALE_WH",
        wh_sched.WarehouseSchedules(
            name="AUTOSCALE_WH",
            size="X-Small",
            suspend_minutes=1,
            resume=True,
            scale_min=4,
            scale_max=4,
            warehouse_mode="Economy",
        ),
    )
    with _mock_task(session, wh_sched_fixture) as task:
        # Run the task
        alter_warehouse_block = task.run(session)

        # Verify the ALTER WAREHOUSE command was correct
        statements = _extract_alter_statements(alter_warehouse_block)
        assert len(statements) == 1
        assert "alter warehouse AUTOSCALE_WH set".lower() in statements[0].lower()
        assert "MIN_CLUSTER_COUNT = 1".lower() in statements[0].lower()
        assert "MAX_CLUSTER_COUNT = 2".lower() in statements[0].lower()
        assert "SCALING_POLICY".lower() not in statements[0].lower()

        # Verify the task result was logged into the internal table
        assert len(wh_sched_fixture.task_log) == 1
        assert wh_sched_fixture.task_log[-1].get(
            "success"
        ), "Task should have succeeded"


def force_economy_mode(df: pd.DataFrame) -> pd.DataFrame:
    df = df.loc[df["NAME"] == "AUTOSCALE_WH"]
    return df.assign(WAREHOUSE_MODE="Economy")


def force_standard_mode(df: pd.DataFrame) -> pd.DataFrame:
    df = df.loc[df["NAME"] == "AUTOSCALE_WH"]
    return df.assign(WAREHOUSE_MODE="Standard")


def test_economy_scaling_policy(session, wh_sched_fixture):
    """
    Economy scaling policy should get set
    """
    # Filter the schedules to only the AUTOSCALE_WH schedules
    wh_sched_fixture.schedule_filter = force_economy_mode

    # Cluster should be set to (1,2)
    wh_sched_fixture.last_task_run = datetime.datetime.combine(
        datetime.date(2023, 9, 26), datetime.time(8, 45)
    )
    wh_sched_fixture.now = datetime.datetime.combine(
        datetime.date(2023, 9, 26), datetime.time(9, 0)
    )

    wh_sched_fixture.override_warehouse_state(
        "AUTOSCALE_WH",
        wh_sched.WarehouseSchedules(
            name="AUTOSCALE_WH",
            size="X-Small",
            suspend_minutes=1,
            resume=True,
            scale_min=4,
            scale_max=4,
            warehouse_mode="Standard",
        ),
    )
    with _mock_task(session, wh_sched_fixture) as task:
        # Run the task
        alter_warehouse_block = task.run(session)

        # Verify the ALTER WAREHOUSE command was correct
        statements = _extract_alter_statements(alter_warehouse_block)
        assert len(statements) == 1
        assert "alter warehouse AUTOSCALE_WH set".lower() in statements[0].lower()
        assert "MIN_CLUSTER_COUNT = 1".lower() in statements[0].lower()
        assert "MAX_CLUSTER_COUNT = 2".lower() in statements[0].lower()
        assert "SCALING_POLICY = Economy".lower() in statements[0].lower()

        # Verify the task result was logged into the internal table
        assert len(wh_sched_fixture.task_log) == 1
        assert wh_sched_fixture.task_log[-1].get(
            "success"
        ), "Task should have succeeded"


def test_standard_scaling_policy(session, wh_sched_fixture):
    """
    Standard scaling policy should get set
    """
    # Filter the schedules to only the AUTOSCALE_WH schedules
    wh_sched_fixture.schedule_filter = force_standard_mode

    # Cluster should be set to (1,2)
    wh_sched_fixture.last_task_run = datetime.datetime.combine(
        datetime.date(2023, 9, 26), datetime.time(8, 45)
    )
    wh_sched_fixture.now = datetime.datetime.combine(
        datetime.date(2023, 9, 26), datetime.time(9, 0)
    )

    wh_sched_fixture.override_warehouse_state(
        "AUTOSCALE_WH",
        wh_sched.WarehouseSchedules(
            name="AUTOSCALE_WH",
            size="X-Small",
            suspend_minutes=1,
            resume=True,
            scale_min=4,
            scale_max=4,
            warehouse_mode="Economy",
        ),
    )
    with _mock_task(session, wh_sched_fixture) as task:
        # Run the task
        alter_warehouse_block = task.run(session)

        # Verify the ALTER WAREHOUSE command was correct
        statements = _extract_alter_statements(alter_warehouse_block)
        assert len(statements) == 1
        assert "alter warehouse AUTOSCALE_WH set".lower() in statements[0].lower()
        assert "MIN_CLUSTER_COUNT = 1".lower() in statements[0].lower()
        assert "MAX_CLUSTER_COUNT = 2".lower() in statements[0].lower()
        assert "SCALING_POLICY = Standard".lower() in statements[0].lower()

        # Verify the task result was logged into the internal table
        assert len(wh_sched_fixture.task_log) == 1
        assert wh_sched_fixture.task_log[-1].get(
            "success"
        ), "Task should have succeeded"


def _extract_alter_statements(alter_block: str, warehouse: str = None) -> List[str]:
    """
    Tries to extract each ALTER WAREHOUSE statement from the snowflake scripting block.
    TODO Could we use sqlglot to parse this?
    """
    if len(alter_block) == 0:
        return []

    statements = alter_block.split("\n")
    assert len(statements) > 2, "Expected at least 3 lines in the ALTER WAREHOUSE block"
    statements = statements[1:-1]
    # Filter down to a specific warehouse
    if warehouse:
        statements = [
            s for s in statements if f"alter warehouse {warehouse.lower()}" in s.lower()
        ]
    return statements


def _no_task_action(alter_block: str, task_log: List[dict]):
    """
    Asserts that the task ran successfully but performed no changes.
    """
    assert not alter_block, "No statements should have been executed"

    # Verify the task result was logged into the internal table
    assert len(task_log) == 1
    assert task_log[0].get("success"), "Task should have succeeded"
    return True
