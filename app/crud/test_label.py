import os
import pytest
from datetime import datetime

from .labels import Label
from .session import session_ctx


class Session:
    def __init__(self):
        self._sql = []

    def sql(self, sql):
        self._sql.append(sql)
        return self

    def collect(self):
        return self


@pytest.fixture(autouse=True)
def session():
    session = Session()
    token = session_ctx.set(session)
    yield session
    session_ctx.reset(token)


def _get_label(name="label1", grouped=False, dynamic=False) -> dict:
    d = dict(
        name=name,
        condition="user_name = 'josh@sundeck.io'",
        modified_at=datetime.now(),
        created_at=datetime.now(),
        enabled=True,
        is_dynamic=dynamic,
    )
    if grouped:
        d['group_name'] = 'group1'
        d['group_rank'] = 1
    return d


def test_label(session):
    l = _get_label()
    _ = Label.parse_obj(l)

    assert len(session._sql) == 2, f"Expected 2 sql statements"
    assert session._sql[0].lower() == _expected_condition_check_query(l.get('condition')), \
        "Unexpected label condition query"
    assert session._sql[1].lower() == _expected_name_check_query(l.get('name')), "Unexpected label name query"


def test_none_label(session):
    l = _get_label(name=None)
    with pytest.raises(ValueError):
        _ = Label.parse_obj(l)

    assert len(session._sql) == 2, "Expected no sql statements for a None name"
    assert session._sql[0].lower() == _expected_condition_check_query(l.get('condition')), \
        "Unexpected label condition query"
    assert session._sql[1].lower() == _expected_name_check_query(l.get('name')), "Unexpected label name query"


def test_empty_label(session):
    l = _get_label(name="")
    with pytest.raises(ValueError):
        _ = Label.parse_obj(l)

    assert len(session._sql) == 2, "Expected no sql statements for a None name"
    assert session._sql[0].lower() == _expected_condition_check_query(l.get('condition')), \
        "Unexpected label condition query"
    # An empty name is overriden to be the default value None.
    assert session._sql[1].lower() == _expected_name_check_query('none'), "Unexpected label name query"


def test_create_table(session):
    Label.create_table(session)
    assert len(session._sql) == 2, "Expected 2 sql statement, got {}".format(
        len(session._sql)
    )
    assert session._sql[0].lower() == " ".join(
        """create table if not exists internal.labels
        (name string null, group_name string null, group_rank number null,
        created_at timestamp, condition string, enabled boolean, modified_at timestamp,
        is_dynamic boolean)""".split()
    ), "Expected create table statement, got {}".format(session._sql[0])
    assert (
        session._sql[1].lower()
        == "create or replace view catalog.labels as select * from internal.labels"
    ), "Expected create view statement, got {}".format(session._sql[1])


def _expected_condition_check_query(condition: str) -> str:
    return f"select case when {condition} then 1 else 0 end from reporting.enriched_query_history where false".lower()

def _expected_name_check_query(name: str) -> str:
    return f'select "{name}" from reporting.enriched_query_history where false'.lower()