import sys
import datetime
import pytest
from contextlib import contextmanager
from common_utils import delete_list_of_labels, delete_list_of_probes

sys.path.append("../deploy")
import helpers  # noqa E402

# Adds --profile option to pytest
def pytest_addoption(parser):
    parser.addoption(
        "--profile",
        action="store",
        default="opscenter",
        help="Connection profile name as specified in ~/.snowsql/config",
    )


# Returns value of --profile argument
def snowsql_profile_name() -> str:
    for i, arg in enumerate(sys.argv):
        if arg == "--profile" and i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return "opscenter"


@contextmanager
def db(profile: str = "default", **kwargs):

    # Get profile from command line argument to pytest
    profile = snowsql_profile_name()

    cnx = helpers.connect_to_snowflake(profile=profile, schema="public")
    try:
        yield cnx
    finally:
        cnx.close()


@pytest.fixture(scope="session", autouse=True)
def conn():
    return db


@pytest.fixture(scope="session", autouse=True)
def timestamp_string(conn):

    # Format the date and time as a timestamp string
    # This is a shared variable that will be used in object names created during test session
    now = datetime.datetime.now()
    ts = now.strftime("%Y-%m-%d-%H-%M-%S")
    print(f"\n[INFO] Timestamp: {ts}")

    yield ts

    # Code that runs once at the end of the test session
    print("\n[INFO] Teardown after running tests in the session")

    sql = f"select name from INTERNAL.LABELS where name like '%{ts}%'"
    print(f"[INFO] SQL stmt to find all the labels: {sql}")

    # call a function that deletes all the labels that were created in the session
    delete_list_of_labels(conn, sql)

    sql = f"select name from INTERNAL.PROBES where name like '%{ts}%'"
    print(f"[INFO] SQL stmt to find all the probes: {sql}")

    # call a function that deletes all the labels that were created in the session
    delete_list_of_probes(conn, sql)
