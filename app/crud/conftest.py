import pytest
import snowflake
from .session import _session_context


class Session:
    """
    A fake snowflake.snowpark.Session object for unit tests.
    """

    def __init__(self):
        self._sql = []

    def sql(self, sql, **kwargs):
        self._sql.append(sql)
        return self

    def collect(self):
        # GROSS. Tricks the tests into passing the check that a label name doesn't conflict with a QUERY_HISTORY column.
        # but only trying to match the name check and not the condition check.
        if (
            self.sql
            and self._sql[-1].endswith(
                "from reporting.enriched_query_history where false"
            )
            and self._sql[-1].startswith('select "')
        ):
            raise snowflake.snowpark.exceptions.SnowparkSQLException(
                "invalid identifier to make tests pass"
            )
        return self


@pytest.fixture(autouse=True)
def session():
    session = Session()
    token = _session_context.set(session)
    yield session
    _session_context.reset(token)
