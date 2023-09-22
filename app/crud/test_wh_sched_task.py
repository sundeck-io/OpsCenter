import datetime
from typing import List
from contextlib import contextmanager
from unittest.mock import patch
from .wh_sched import WarehouseSchedulesTask
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
        # self.get_schedules()
        mocked_get_schedules.return_value = fixtures.get_schedules()
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
    # Set the last time the task ran and "now" to match the test data.
    wh_sched_fixture.last_task_run = datetime.datetime.combine(
        datetime.date.today(), datetime.time(8, 45)
    )
    wh_sched_fixture.now = datetime.datetime.combine(
        datetime.date.today(), datetime.time(9, 0)
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
