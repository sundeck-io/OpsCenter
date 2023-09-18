from contextvars import ContextVar
from contextlib import contextmanager

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


def get_current_session():
    return _session_context.get()

