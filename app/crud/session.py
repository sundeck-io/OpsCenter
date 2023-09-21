from contextvars import ContextVar
from contextlib import contextmanager
from snowflake.snowpark import Session

_session_context = ContextVar("session")


@contextmanager
def snowpark_session(session):
    """
    Makes the give Snowpark Session available to the CRUD implementation for the scope of this call.
    """
    token = _session_context.set(session)
    try:
        yield session
    finally:
        _session_context.reset(token)


def get_current_session() -> Session:
    s = _session_context.get()
    if not s:
        raise ValueError("Session must be set by caller")
    return s
