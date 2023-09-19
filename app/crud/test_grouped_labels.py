import pytest

from .labels import Label
from .test_label import (
    _get_label,
    _expected_condition_check_query,
    _expected_name_check_query,
)


def _get_grouped_label(
    group_name: str = "mygroup", group_rank: int = 25, **kwargs
) -> dict:
    return _get_label(group_name=group_name, group_rank=group_rank, **kwargs)


def test_grouped_label(session):
    test_label = _get_grouped_label()
    _ = Label.parse_obj(test_label)

    assert len(session._sql) == 2, "Expected 2 sql statements"
    assert session._sql[0].lower() == _expected_condition_check_query(
        test_label.get("condition")
    ), "Unexpected label condition query"
    assert session._sql[1].lower() == _expected_name_check_query(
        test_label.get("group_name")
    ), "Unexpected label group name query"


def test_condition_required(session):
    # Condition is required
    with pytest.raises(ValueError):
        test_label = _get_grouped_label(condition="")
        _ = Label.parse_obj(test_label)


def test_cannot_be_dynamic(session):
    # Group label cannot be dynamic
    with pytest.raises(ValueError):
        test_label = _get_grouped_label()
        test_label["is_dynamic"] = True
        _ = Label.parse_obj(test_label)


def test_needs_created_at(session):
    # Created_at is required
    with pytest.raises(ValueError):
        test_label = _get_grouped_label()
        del test_label["label_created_at"]
        _ = Label.parse_obj(test_label)


def test_needs_modified_at(session):
    # Modified_at is required
    with pytest.raises(ValueError):
        test_label = _get_grouped_label()
        del test_label["label_modified_at"]
        _ = Label.parse_obj(test_label)


def test_needs_group_rank(session):
    # Remove group_rank
    with pytest.raises(ValueError):
        test_label = _get_grouped_label()
        del test_label["group_rank"]
        _ = Label.parse_obj(test_label)
