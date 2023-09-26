import datetime
from typing import List
from contextlib import contextmanager
from unittest.mock import patch
from .wh_sched import WarehouseSchedules, WarehouseSchedulesTask
from .test_fixtures import WarehouseScheduleFixture


@contextmanager
def _mock_task(session, fixtures: WarehouseScheduleFixture) -> WarehouseSchedulesTask:
    with patch.object(
        WarehouseSchedulesTask, "get_last_run"
    ) as mocked_get_last_run, patch.object(
        WarehouseSchedulesTask, "now"
    ) as mocked_now, patch.object(
        WarehouseSchedulesTask, "get_schedules"
    ) as mocked_get_schedules, patch.object(
        WarehouseSchedulesTask, "describe_warehouse"
    ) as mock_describe_warehouse, patch.object(
        WarehouseSchedulesTask, "log_to_table"
    ) as mock_log_to_table, patch.object(
        WarehouseSchedulesTask, "run_statement"
    ) as mock_run_statement:
        # self.now()
        mocked_now.return_value = fixtures.now
        # self.last_task_run()
        mocked_get_last_run.return_value = fixtures.last_task_run
        # self.get_schedules(is_weekday)
        mocked_get_schedules.side_effect = (
            lambda *args, **kwards: fixtures.get_schedules(args[0])
        )
        # self.describe_warehouse(str)
        mock_describe_warehouse.side_effect = (
            lambda *args, **kwargs: fixtures.describe_warehouse(args[0])
        )
        # self.log_to_table(this_run, success, obj)
        mock_log_to_table.side_effect = lambda *args, **kwargs: fixtures.log_task_run(
            args[0], args[1], args[2]
        )
        # self.run_statement(stmt)
        mock_run_statement.side_effect = lambda *args, **kwargs: fixtures.run_statement(
            args[0]
        )

        task = WarehouseSchedulesTask(session)

        yield task


def test_basic_task(session, wh_sched_fixture):
    # Set the last time the task ran and "now" to a weekday that matches the test data.
    wh_sched_fixture.last_task_run = datetime.datetime.combine(
        datetime.date(2023, 9, 25), datetime.time(8, 45)
    )
    wh_sched_fixture.now = datetime.datetime.combine(
        datetime.date(2023, 9, 26), datetime.time(9, 0)
    )

    with _mock_task(session, wh_sched_fixture) as task:
        # Run the task
        alter_warehouse_block = task.run()

        # Verify the ALTER WAREHOUSE command was correct
        statements = _extract_alter_statements(alter_warehouse_block)
        assert len(statements) == 1
        assert "alter warehouse COMPUTE_WH set".lower() in statements[0].lower()
        assert "WAREHOUSE_SIZE = XLARGE".lower() in statements[0].lower()
        assert "AUTO_SUSPEND = 15".lower() in statements[0].lower()

        # Verify the task result was logged into the internal table
        assert len(wh_sched_fixture.task_log) == 1
        assert wh_sched_fixture.task_log[0].get("success"), "Task should have succeeded"


def test_noop_weekday_task(session, wh_sched_fixture):
    """
    Over the weekend, the task should do nothing if the warehouse doesn't need change.
    """
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
        WarehouseSchedules(
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
        alter_warehouse_block = task.run()
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
        alter_warehouse_block = task.run()
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
        WarehouseSchedules(
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
        alter_warehouse_block = task.run()

        # Verify the ALTER WAREHOUSE command was correct
        statements = _extract_alter_statements(alter_warehouse_block)
        assert len(statements) == 1
        assert "alter warehouse COMPUTE_WH set".lower() in statements[0].lower()
        assert "WAREHOUSE_SIZE = XSMALL".lower() in statements[0].lower()
        assert "AUTO_SUSPEND = 1".lower() in statements[0].lower()

        # Verify the task result was logged into the internal table
        assert len(wh_sched_fixture.task_log) == 1
        assert wh_sched_fixture.task_log[0].get("success"), "Task should have succeeded"


def test_basic_weekday_task(session, wh_sched_fixture):
    """
    Make sure the weekday schedule instead of the weekend task.
    """
    # First task run on the weekend.
    wh_sched_fixture.last_task_run = datetime.datetime.combine(
        datetime.date(2023, 9, 25), datetime.time(23, 45)
    )
    wh_sched_fixture.now = datetime.datetime.combine(
        datetime.date(2023, 9, 26), datetime.time(0, 0)
    )

    # Tweak the current state of COMPUTE_WH so the task definitely does something.
    wh_sched_fixture.override_warehouse_state(
        "COMPUTE_WH",
        WarehouseSchedules(
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
        alter_warehouse_block = task.run()

        # Verify the ALTER WAREHOUSE command was correct
        statements = _extract_alter_statements(alter_warehouse_block)
        assert len(statements) == 1
        assert "alter warehouse COMPUTE_WH set".lower() in statements[0].lower()
        assert "WAREHOUSE_SIZE = SMALL".lower() in statements[0].lower()
        assert "AUTO_SUSPEND = 1".lower() in statements[0].lower()

        # Verify the task result was logged into the internal table
        assert len(wh_sched_fixture.task_log) == 1
        assert wh_sched_fixture.task_log[0].get("success"), "Task should have succeeded"


def _extract_alter_statements(alter_block: str) -> List[str]:
    """
    Tries to extract each ALTER WAREHOUSE statement from the snowflake scripting block.
    TODO Could we use sqlglot to parse this?
    """
    if len(alter_block) == 0:
        return []

    statements = alter_block.split("\n")
    assert len(statements) > 2, "Expected at least 3 lines in the ALTER WAREHOUSE block"
    return statements[1:-1]


def _no_task_action(alter_block: str, task_log: List[dict]):
    """
    Asserts that the task ran successfully but performed no changes.
    """
    assert not alter_block, "No statements should have been executed"

    # Verify the task result was logged into the internal table
    assert len(task_log) == 1
    assert task_log[0].get("success"), "Task should have succeeded"
    return True
