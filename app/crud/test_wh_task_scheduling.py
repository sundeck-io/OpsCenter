import datetime
from .wh_sched import WarehouseSchedules, update_task_state
from .test_fixtures import MockSession


def _make_schedule(
    name: str,
    start_at: datetime.time,
    finish_at: datetime.time,
    weekday=True,
    enabled=True,
) -> WarehouseSchedules:
    return WarehouseSchedules(
        name=name,
        start_at=start_at,
        finish_at=finish_at,
        size="X-Small",
        suspend_minutes=1,
        resume=True,
        scale_min=0,
        scale_max=0,
        warehouse_mode="Inherit",
        weekday=weekday,
        enabled=enabled,
    )


def test_no_schedules_disables_task(session: MockSession):
    assert not update_task_state(session, [])
    assert len(session._sql) == 1
    assert (
        session._sql[0].lower()
        == "alter task if exists tasks.warehouse_scheduling suspend"
    )


def test_disabled_schedules_disables_task(session: MockSession):
    schedules = [
        _make_schedule(
            "COMPUTE_WH", datetime.time(0, 0), datetime.time(23, 59), enabled=False
        ),
    ]

    assert not update_task_state(session, schedules)
    assert len(session._sql) == 1
    assert (
        session._sql[0].lower()
        == "alter task if exists tasks.warehouse_scheduling suspend"
    )


def test_weekday_schedules(session: MockSession):
    schedules = [
        _make_schedule("COMPUTE_WH", datetime.time(0, 0), datetime.time(9, 0)),
        _make_schedule("COMPUTE_WH", datetime.time(0, 0), datetime.time(17, 0)),
    ]

    assert update_task_state(session, schedules)
    assert len(session._sql) == 1
    script = session._sql[0].lower()

    assert "alter task if exists tasks.warehouse_scheduling resume" in script

    expected_cron = "0 9,17 * * * 1-5"
    assert f"set schedule = 'using cron {expected_cron}'" in script
