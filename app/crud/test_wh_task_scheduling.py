import datetime
from .wh_sched import WarehouseSchedules, WarehouseSchedulesTask, update_task_state
from .test_fixtures import MockSession


def _make_schedule(
    name: str,
    start_at: datetime.time,
    finish_at: datetime.time,
    size="X-Small",
    suspend_minutes=1,
    weekday=True,
    enabled=True,
) -> WarehouseSchedules:
    return WarehouseSchedules(
        name=name,
        start_at=start_at,
        finish_at=finish_at,
        size=size,
        suspend_minutes=suspend_minutes,
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
    for offset in WarehouseSchedulesTask.task_offsets:
        assert (
            f"alter task if exists tasks.warehouse_scheduling_{offset} suspend;"
            in session._sql[0].lower()
        )


def test_disabled_schedules_disables_task(session: MockSession):
    schedules = [
        _make_schedule(
            "COMPUTE_WH", datetime.time(0, 0), datetime.time(23, 59), enabled=False
        ),
    ]

    assert not update_task_state(session, schedules)
    assert len(session._sql) == 1
    for offset in WarehouseSchedulesTask.task_offsets:
        assert (
            f"alter task if exists tasks.warehouse_scheduling_{offset} suspend;"
            in session._sql[0].lower()
        )


def test_weekday_schedules(session: MockSession):
    schedules = [
        _make_schedule("COMPUTE_WH", datetime.time(0, 0), datetime.time(9, 0)),
        _make_schedule("COMPUTE_WH", datetime.time(9, 0), datetime.time(17, 30)),
        _make_schedule("COMPUTE_WH", datetime.time(17, 30), datetime.time(23, 59)),
    ]

    assert update_task_state(session, schedules) is True
    assert len(session._sql) == 1
    script = session._sql[0].lower()

    # _0 and _30 should be resumed
    assert "alter task if exists tasks.warehouse_scheduling_0 suspend;" in script

    expected_cron_0 = "0 0,9 * * 1-5 america/los_angeles"
    assert (
        f"alter task if exists tasks.warehouse_scheduling_0 set schedule = 'using cron {expected_cron_0}';"
        in script
    )
    assert "alter task if exists tasks.warehouse_scheduling_0 resume;" in script

    assert "alter task if exists tasks.warehouse_scheduling_30 suspend;" in script
    assert "alter task if exists tasks.warehouse_scheduling_30 resume;" in script

    # _15 and _45 should be suspended
    assert "alter task if exists tasks.warehouse_scheduling_15 suspend;" in script
    assert "alter task if exists tasks.warehouse_scheduling_45 suspend;" in script

    expected_cron_30 = "30 17 * * 1-5 america/los_angeles"
    assert (
        f"alter task if exists tasks.warehouse_scheduling_30 set schedule = 'using cron {expected_cron_30}';"
        in script
    )


def test_weekdays_and_weekends_schedule(session: MockSession):
    schedules = [
        # Separate workday mornings and evenings, from non-working weekday hours.
        _make_schedule("COMPUTE_WH", datetime.time(0, 0), datetime.time(9, 0)),
        _make_schedule("COMPUTE_WH", datetime.time(9, 0), datetime.time(12, 30)),
        _make_schedule("COMPUTE_WH", datetime.time(12, 30), datetime.time(17, 45)),
        _make_schedule("COMPUTE_WH", datetime.time(17, 45), datetime.time(23, 59)),
        # ETL runs during evenings on the weekends
        _make_schedule(
            "COMPUTE_WH",
            datetime.time(0, 0),
            datetime.time(21, 45),
            size="X-Small",
            weekday=False,
        ),
        _make_schedule(
            "COMPUTE_WH",
            datetime.time(21, 45),
            datetime.time(23, 59),
            size="X-Large",
            weekday=False,
        ),
    ]

    assert update_task_state(session, schedules) is True
    assert len(session._sql) == 1
    script = session._sql[0].lower()

    # _0, _30, and _45 should be resumed
    assert "alter task if exists tasks.warehouse_scheduling_0 resume;" in script
    assert "alter task if exists tasks.warehouse_scheduling_30 resume;" in script
    assert "alter task if exists tasks.warehouse_scheduling_45 resume;" in script

    # _15 should be suspended
    assert "alter task if exists tasks.warehouse_scheduling_15 suspend;" in script

    expected_cron_0 = "0 0,9 * * * america/los_angeles"
    assert (
        f"alter task if exists tasks.warehouse_scheduling_0 set schedule = 'using cron {expected_cron_0}';"
        in script
    )

    expected_cron_30 = "30 12 * * 1-5 america/los_angeles"
    assert (
        f"alter task if exists tasks.warehouse_scheduling_30 set schedule = 'using cron {expected_cron_30}';"
        in script
    )

    expected_cron_45 = "45 17,21 * * * america/los_angeles"
    assert (
        f"alter task if exists tasks.warehouse_scheduling_45 set schedule = 'using cron {expected_cron_45}';"
        in script
    )


def test_multiple_warehouses(session: MockSession):
    schedules = [
        #  COMPUTE_WH
        _make_schedule("COMPUTE_WH", datetime.time(0, 0), datetime.time(9, 0)),
        _make_schedule("COMPUTE_WH", datetime.time(9, 0), datetime.time(12, 30)),
        _make_schedule("COMPUTE_WH", datetime.time(12, 30), datetime.time(17, 45)),
        _make_schedule("COMPUTE_WH", datetime.time(17, 45), datetime.time(23, 59)),
        # AUTOSCALE_WH
        _make_schedule("AUTOSCALE_WH", datetime.time(0, 0), datetime.time(22, 0)),
        _make_schedule("AUTOSCALE_WH", datetime.time(22, 0), datetime.time(23, 59)),
        # BATCH_WH
        _make_schedule("BATCH_WH", datetime.time(0, 0), datetime.time(10, 0)),
        _make_schedule("BATCH_WH", datetime.time(10, 30), datetime.time(11, 0)),
        _make_schedule("BATCH_WH", datetime.time(11, 0), datetime.time(11, 15)),
        _make_schedule("BATCH_WH", datetime.time(11, 15), datetime.time(12, 45)),
        _make_schedule("BATCH_WH", datetime.time(12, 45), datetime.time(23, 59)),
    ]

    assert update_task_state(session, schedules) is True
    assert len(session._sql) == 1
    script = session._sql[0].lower()

    # All tasks are running
    for offset in WarehouseSchedulesTask.task_offsets:
        assert (
            f"alter task if exists tasks.warehouse_scheduling_{offset} resume;"
            in script
        )

    expected_cron_0 = "0 0,9,11,22 * * 1-5 america/los_angeles"
    assert (
        f"alter task if exists tasks.warehouse_scheduling_0 set schedule = 'using cron {expected_cron_0}';"
        in script
    )

    expected_cron_15 = "15 11 * * 1-5 america/los_angeles"
    assert (
        f"alter task if exists tasks.warehouse_scheduling_15 set schedule = 'using cron {expected_cron_15}';"
        in script
    )

    expected_cron_30 = "30 10,12 * * 1-5 america/los_angeles"
    assert (
        f"alter task if exists tasks.warehouse_scheduling_30 set schedule = 'using cron {expected_cron_30}';"
        in script
    )

    expected_cron_45 = "45 12,17 * * 1-5 america/los_angeles"
    assert (
        f"alter task if exists tasks.warehouse_scheduling_45 set schedule = 'using cron {expected_cron_45}';"
        in script
    )
