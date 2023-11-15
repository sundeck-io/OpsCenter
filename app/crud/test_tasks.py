import pytest
from pydantic import ValidationError
from .conftest import MockSession
from .tasks import Task


@pytest.mark.parametrize(
    "name",
    [
        ("QUERY_HISTORY_MAINTENANCE"),
        ("query_history_maintenance"),
        ("WaREHouse_EVENTS_maintenance "),
        ("SFUSER_MAINTENANCE"),
        ("USER_LIMITS_MAINTENANCE"),
    ],
)
def test_task_enable(session: MockSession, name: str):
    task = Task(task_name=name)
    task.enable(session)

    expected_name = name.strip().lower()
    assert len(session._sql) == 1, "Expected 1 sql statement"
    assert (
        session._sql[0].lower() == f"alter task tasks.{expected_name} resume"
    ), "Unexpected task enable query"

    task.disable(session)

    assert len(session._sql) == 2, "Expected 2 sql statements"
    assert (
        session._sql[1].lower() == f"alter task tasks.{expected_name} suspend"
    ), "Unexpected task disable query"


def test_unknown_task(session: MockSession):
    with pytest.raises(ValidationError):
        _ = Task(task_name="UNKNOWN_TASK")
