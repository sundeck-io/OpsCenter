from contextvars import ContextVar
from contextlib import contextmanager

_session_contextvar = ContextVar("session")


@contextmanager
def snowpark_session(session):
    """
    Makes the give Snowpark Session available to the CRUD implementation for the scope of this call.
    """
    token = _session_contextvar.set(session)
    try:
        yield session
    finally:
        _session_contextvar.reset(token)


def get_current_session():
    return _session_contextvar.get()