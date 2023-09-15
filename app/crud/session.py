from contextvars import ContextVar
from contextlib import contextmanager

session_ctx = ContextVar('session')


@contextmanager
def snowpark_session(session):
    """
    Makes the give Snowpark Session available to the CRUD implementation for the scope of this call.
    :param session:
    :return:
    """
    token = session_ctx.set(session)
    yield session
    session_ctx.reset(token)