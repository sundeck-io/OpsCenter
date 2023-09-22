import pytest
from .session import _session_context
from .test_fixtures import MockSession, WarehouseScheduleFixture


@pytest.fixture(autouse=True)
def session():
    session = MockSession()
    token = _session_context.set(session)
    yield session
    _session_context.reset(token)


@pytest.fixture()
def wh_sched_fixture():
    return WarehouseScheduleFixture()
