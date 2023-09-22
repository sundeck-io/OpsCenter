import datetime
import pytest
from .wh_sched import WarehouseSchedules


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
