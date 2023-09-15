import pytest

from .labels import Label
from .session import session_ctx
from .test_label import Session, _get_label, _expected_condition_check_query, _expected_name_check_query

@pytest.fixture(autouse=True)
def session():
    session = Session()
    token = session_ctx.set(session)
    yield session
    session_ctx.reset(token)


def test_grouped_label(session):
    l = _get_label(group_rank=25)
    _ = Label.parse_obj(l)

    assert len(session._sql) == 2, f"Expected 2 sql statements"
    assert session._sql[0].lower() == _expected_condition_check_query(l.get('condition')), \
        "Unexpected label condition query"
    assert session._sql[1].lower() == _expected_name_check_query(l.get('group_name')), "Unexpected label group name query"


def test_condition_required(session):
    # Condition is required
    with pytest.raises(ValueError):
        l = _get_label(group_rank=25, condition="")
        _ = Label.parse_obj(l)


def test_cannot_be_dynamic(session):
    # Group label cannot be dynamic
    with pytest.raises(ValueError):
        l = _get_label(group_rank=25)
        l['is_dynamic'] = True
        _ = Label.parse_obj(l)


def test_needs_created_at(session):
    # Created_at is required
    with pytest.raises(ValueError):
        l = _get_label(group_rank=25)
        del l['label_created_at']
        _ = Label.parse_obj(l)


def test_needs_modified_at(session):
    # Modified_at is required
    with pytest.raises(ValueError):
        l = _get_label(group_rank=25)
        del l['label_modified_at']
        _ = Label.parse_obj(l)


def test_cannot_have_name_when_grouped(session):
    # Name is not allowed for grouped labels, should be group_name
    with pytest.raises(ValueError):
        l = _get_label(group_rank=25)
        l['name'] = 'asdf'
        _ = Label.parse_obj(l)


def test_needs_group_rank(session):
    # Remove group_rank
    with pytest.raises(ValueError):
        l = _get_label(group_rank=25)
        del l['group_rank']
        _ = Label.parse_obj(l)
