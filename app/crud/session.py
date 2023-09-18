from contextvars import ContextVar
from contextlib import contextmanager

session_ctx = ContextVar("session")
operation_context = ContextVar("operation")


@contextmanager
def snowpark_session(session):
    """
    Makes the give Snowpark Session available to the CRUD implementation for the scope of this call.
    """
    token = session_ctx.set(session)
    try:
        yield session
    finally:
        session_ctx.reset(token)


@contextmanager
def operation(name):
    """
    Makes the given operation name available to the CRUD implementation for the scope of this call.
    """
    token = operation_context.set(name)
    try:
        yield name
    finally:
        operation_context.reset(token)