from contextvars import ContextVar
from contextlib import contextmanager

session_context = ContextVar("session")


@contextmanager
def snowpark_session(session):
    """
    Makes the give Snowpark Session available to the CRUD implementation for the scope of this call.
    """
    token = session_context.set(session)
    try:
        yield session
    finally:
        session_context.reset(token)
