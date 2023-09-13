import pytest
from labels import Label
from datetime import datetime


class Session:
    def __init__(self):
        self._sql = []

    def sql(self, sql):
        self._sql.append(sql)
        return self

    def collect(self):
        return self


@pytest.fixture
def session():
    return Session()


def _get_label(name="label1") -> dict:
    return dict(
        name=name,
        condition="user_name = 'josh@sundeck.io'",
        modified_at=datetime.now(),
        created_at=datetime.now(),
        enabled=True,
        is_dynamic=False,
    )


def test_label(session):
    _ = Label.model_validate(_get_label(), context={"session": session})
    # todo validate session.sql was called


def test_none_label(session):
    with pytest.raises(ValueError):
        _ = Label.model_validate(_get_label(name=None), context={"session": session})
    # todo validate session.sql was called


def test_empty_label(session):
    with pytest.raises(ValueError):
        _ = Label.model_validate(_get_label(name=""), context={"session": session})
    # todo validate session.sql was called


def test_create(session):
    Label.create(session)
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
