import datetime
import pytest
from typing import List
from .wh_sched import (
    WarehouseSchedules,
    delete_warehouse_schedule,
    merge_new_schedule,
    verify_and_clean,
)


def _assert_contiguous(schedules: List[WarehouseSchedules]):
    if len(schedules) == 0:
        assert schedules[0].start_at == datetime.time(0, 0)
        assert schedules[0].finish_at == datetime.time(23, 59)
        return

    prev = schedules[0]
    assert schedules[0].start_at == datetime.time(0, 0)
    for s in schedules[1:]:
        assert prev.finish_at == s.start_at, f"Schedules were {schedules}"
        prev = s
    assert schedules[-1].finish_at == datetime.time(
        23, 59
    ), f"Schedules were {schedules}"


def _make_schedule(
    name: str,
    start_at: datetime.time,
    finish_at: datetime.time,
    weekday: bool,
    size="X-Small",
    suspend_minutes=1,
    resume=True,
    scale_min=0,
    scale_max=0,
    warehouse_mode="Standard",
    enabled=True,
):
    return WarehouseSchedules(
        name=name,
        weekday=weekday,
        size=size,
        suspend_minutes=suspend_minutes,
        resume=resume,
        scale_min=scale_min,
        scale_max=scale_max,
        warehouse_mode=warehouse_mode,
        start_at=start_at,
        finish_at=finish_at,
        enabled=enabled,
    )


def _get_wh_sched(
    size="X-Small",
    suspend_minutes=1,
    resume=True,
    scale_min=0,
    scale_max=0,
    warehouse_mode="Standard",
    name="bob",
):
    return dict(
        name=name,
        size=size,
        suspend_minutes=suspend_minutes,
        resume=resume,
        scale_min=scale_min,
        scale_max=scale_max,
        warehouse_mode=warehouse_mode,
    )


def test_basic_wh_sched():
    test_wh_sched = _get_wh_sched()
    _ = WarehouseSchedules.parse_obj(test_wh_sched)


def test_bad_size_wh_sched():
    test_wh_sched = _get_wh_sched(size="Foo")
    with pytest.raises(ValueError):
        _ = WarehouseSchedules.parse_obj(test_wh_sched)


def test_bad_times_sched():
    test_wh_sched = _get_wh_sched()
    test_wh_sched["start_at"] = datetime.time.max
    with pytest.raises(ValueError):
        _ = WarehouseSchedules.parse_obj(test_wh_sched)
    test_wh_sched["start_at"] = datetime.time(1, 0)
    test_wh_sched["finish_at"] = datetime.time(0, 0)
    with pytest.raises(ValueError):
        _ = WarehouseSchedules.parse_obj(test_wh_sched)


def test_bad_scales():
    test_wh_sched = _get_wh_sched(scale_min=10)
    with pytest.raises(ValueError):
        _ = WarehouseSchedules.parse_obj(test_wh_sched)


def test_streamlit_values_without_autoscaling():
    no_autoscaling = WarehouseSchedules(
        name="COMPUTE_WH",
        size="X-Small",
        suspend_minutes=1,
        resume=True,
        scale_min=0,
        scale_max=0,
        warehouse_mode="Standard",
    )

    assert not no_autoscaling.autoscaling_enabled()
    assert (
        no_autoscaling.st_min_cluster_value() == 0
    ), "Zero should be preserved if autoscaling is not enabled"
    assert (
        no_autoscaling.st_min_cluster_minvalue() == 0
    ), "Zero should be preserved if autoscaling is not enabled"
    assert (
        no_autoscaling.st_max_cluster_value() == 0
    ), "Max cluster size should be zero if autoscaling is not enabled"
    assert (
        no_autoscaling.st_max_cluster_maxvalue() == 0
    ), "Max value is zero if autoscaling is not enabled"


def test_streamlit_values_with_autoscaling():
    autoscaling = WarehouseSchedules(
        name="COMPUTE_WH",
        size="X-Small",
        suspend_minutes=1,
        resume=True,
        scale_min=1,
        scale_max=3,
        warehouse_mode="Economy",
    )

    assert autoscaling.autoscaling_enabled()
    assert (
        autoscaling.st_min_cluster_value() == 1
    ), "Should return the scale_min value when autoscaling is enabled"
    assert (
        autoscaling.st_min_cluster_minvalue() == 1
    ), "With autoscaling enabled, one is the minimum size"
    assert (
        autoscaling.st_max_cluster_value() == 3
    ), "Should return the scale_max value when autoscaling is enabled"
    assert (
        autoscaling.st_max_cluster_maxvalue() == 10
    ), "10 is the largest allowed value"


def test_create_and_merge_default_schedule():
    schedules = [
        _make_schedule("COMPUTE_WH", datetime.time(0, 0), datetime.time(23, 59), True),
    ]

    ws = _make_schedule("COMPUTE_WH", datetime.time(9, 0), datetime.time(23, 59), True)

    merged = merge_new_schedule(ws, schedules)
    assert len(merged) == 2
    _assert_contiguous(merged)

    err, _ = verify_and_clean(merged)
    assert err is None


def test_create_and_merge_insert_middle():
    schedules = [
        _make_schedule("COMPUTE_WH", datetime.time(0, 0), datetime.time(9, 0), True),
        _make_schedule("COMPUTE_WH", datetime.time(9, 0), datetime.time(17, 0), True),
        _make_schedule("COMPUTE_WH", datetime.time(17, 0), datetime.time(23, 59), True),
    ]

    ws = _make_schedule("COMPUTE_WH", datetime.time(12, 0), datetime.time(17, 00), True)

    merged = merge_new_schedule(ws, schedules)
    assert len(merged) == 4
    _assert_contiguous(merged)
    assert merged[-2].start_at == datetime.time(12, 0)
    assert merged[-2].finish_at == datetime.time(17, 0)

    err, _ = verify_and_clean(merged)
    assert err is None


def test_create_and_merge_insert_at_end():
    schedules = [
        _make_schedule("COMPUTE_WH", datetime.time(0, 0), datetime.time(9, 0), True),
        _make_schedule("COMPUTE_WH", datetime.time(9, 0), datetime.time(17, 0), True),
        _make_schedule("COMPUTE_WH", datetime.time(17, 0), datetime.time(23, 59), True),
    ]

    ws = _make_schedule("COMPUTE_WH", datetime.time(21, 0), datetime.time(23, 59), True)

    merged = merge_new_schedule(ws, schedules)
    assert len(merged) == 4
    _assert_contiguous(merged)
    assert merged[-1].start_at == datetime.time(21, 0)

    err, _ = verify_and_clean(merged)
    assert err is None


def test_merge_new_schedule_on_empty():
    ws = _make_schedule("COMPUTE_WH", datetime.time(0, 0), datetime.time(23, 59), True)

    merged = merge_new_schedule(ws, [])
    assert len(merged) == 1
    _assert_contiguous(merged)

    err, _ = verify_and_clean(merged)
    assert err is None


def test_create_new_schedules_bad_times():
    # Test that schedules which do not overlap an entire day are rejected
    with pytest.raises(ValueError):
        ws = _make_schedule(
            "COMPUTE_WH", datetime.time(12, 0), datetime.time(23, 59), True
        )
        merge_new_schedule(ws, [])

    with pytest.raises(ValueError):
        ws = _make_schedule(
            "COMPUTE_WH", datetime.time(0, 0), datetime.time(12, 0), True
        )
        merge_new_schedule(ws, [])

    with pytest.raises(ValueError):
        ws = _make_schedule(
            "COMPUTE_WH", datetime.time(0, 0), datetime.time(23, 58), True
        )
        merge_new_schedule(ws, [])


def test_insert_after_first():
    schedules = [
        _make_schedule("COMPUTE_WH", datetime.time(0, 0), datetime.time(17, 0), True),
        _make_schedule("COMPUTE_WH", datetime.time(17, 0), datetime.time(23, 59), True),
    ]

    ws = _make_schedule("COMPUTE_WH", datetime.time(9, 0), datetime.time(17, 00), True)

    merged = merge_new_schedule(ws, schedules)
    assert len(merged) == 3
    _assert_contiguous(merged)
    assert merged[1].start_at == datetime.time(9, 0), f"Schedules were {merged}"
    assert merged[2].start_at == datetime.time(17, 0), f"Schedules were {merged}"

    err, _ = verify_and_clean(merged)
    assert err is None


def test_delete_last_schedule():
    schedules = [
        _make_schedule("COMPUTE_WH", datetime.time(0, 0), datetime.time(9, 0), True),
        _make_schedule("COMPUTE_WH", datetime.time(9, 0), datetime.time(17, 0), True),
        _make_schedule("COMPUTE_WH", datetime.time(17, 0), datetime.time(23, 59), True),
    ]

    del schedules[2]

    err, new_schedules = verify_and_clean(schedules, ignore_errors=True)
    assert err is None
    assert len(new_schedules) == 1
    assert new_schedules[0].start_at == datetime.time(9, 0)
    assert new_schedules[0].finish_at == datetime.time(23, 59)


def test_try_delete_all_schedules():
    with pytest.raises(ValueError):
        s = _make_schedule(
            "COMPUTE_WH", datetime.time(0, 0), datetime.time(23, 59), True
        )
        delete_warehouse_schedule(s, [s])
