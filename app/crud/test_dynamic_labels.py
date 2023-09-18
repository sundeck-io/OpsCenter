import pytest

from .labels import Label
from .session import session_ctx
from .test_label import Session, _get_label, _expected_name_check_query


@pytest.fixture(autouse=True)
def session():
    session = Session()
    token = session_ctx.set(session)
    yield session
    session_ctx.reset(token)


def _get_dynamic_label() -> dict:
    return _get_label(condition="QUERY_TYPE", dynamic=True)


def test_dynamic_label(session):
    dl = _get_label("dynamic_label", condition="QUERY_TYPE", dynamic=True)
    Label.parse_obj(dl)

    assert len(session._sql) == 2, "Expected 2 sql statements"
    assert session._sql[0].lower() == _expected_dynamic_label_condition_check_query(
        dl.get("condition")
    ), "Unexpected dynamic label condition query"
    assert session._sql[1].lower() == _expected_name_check_query(
        dl.get("group_name")
    ), "Unexpected label name query"


def test_name_is_disallowed(session):
    # Name is disallowed for dynamic labels, they use group_name
    with pytest.raises(ValueError):
        dl = _get_dynamic_label()
        dl["name"] = "Something"
        Label.parse_obj(dl)


def test_group_rank_is_disallowed(session):
    # Group rank is disallowed for dynamic labels
    with pytest.raises(ValueError):
        dl = _get_dynamic_label()
        dl["group_rank"] = 50
        Label.parse_obj(dl)


def test_needs_created_at(session):
    # Created_at is required
    with pytest.raises(ValueError):
        dl = _get_dynamic_label()
        del dl["label_created_at"]
        _ = Label.parse_obj(dl)


def test_needs_modified_at(session):
    # Modified_at is required
    with pytest.raises(ValueError):
        dl = _get_dynamic_label()
        del dl["label_modified_at"]
        _ = Label.parse_obj(dl)


def _expected_dynamic_label_condition_check_query(condition: str) -> str:
    return f"select substring({condition}, 0, 0) from reporting.enriched_query_history where false".lower()
