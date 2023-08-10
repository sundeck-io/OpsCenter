import os
import sys
import datetime
import pytest
import snowflake.connector

from contextlib import contextmanager
from common_utils import delete_list_of_labels
from configparser import RawConfigParser

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


def get_db_parameters_from_snowsql_config(profile):
    """
    Sets the database connection parameters based on profile defined in ~/.snowsql/config
    It can have multiple profiles that look like this:

    [connections.local_dev]
    accountname=xyz12345
    username="vicky"
    password="***"
    warehousename ="COMPUTE_WH"
    dbname = "testdb"
    """

    # Define a function to remove quotes from a string
    def remove_quotes(string):
        if string.startswith('"'):
            string = string[1:]
        if string.endswith('"'):
            string = string[:-1]
        return string

    # Define the path to the SnowSQL config file
    config_path = os.path.expanduser("~/.snowsql/config")

    # Name in snowsql connection secion
    profile = snowsql_profile_name()

    # Create a ConfigParser object and read the config file
    config = RawConfigParser()
    config.read(config_path)

    if not config.has_section(f"connections.{profile}"):
        raise ValueError(
            f"Profile {profile} not found in SnowSQL config file at {config_path}"
        )

    # Get the accountname, username, and password properties from the [connections] section
    accountname = remove_quotes(config.get(f"connections.{profile}", "accountname"))
    username = remove_quotes(config.get(f"connections.{profile}", "username"))
    password = remove_quotes(config.get(f"connections.{profile}", "password"))
    warehousename = remove_quotes(config.get(f"connections.{profile}", "warehousename"))
    dbname = remove_quotes(config.get(f"connections.{profile}", "dbname"))
    region = remove_quotes(config.get(f"connections.{profile}", "region", fallback=""))
    schema = "public"

    if len(dbname) == 0:
        raise ValueError(f"Database must be specified in config connections.{profile}")

    db_params = {
        "user": username,
        "password": password,
        "account": accountname,
        "warehousename": warehousename,
        "database": dbname,
        "schema": schema,
        "region": region,
    }

    return db_params


def create_connection(profile, **kwargs):
    ret = get_db_parameters_from_snowsql_config(profile)
    ret.update(kwargs)
    connection = snowflake.connector.connect(**ret)
    return connection


@contextmanager
def db(profile: str = "default", **kwargs):

    cnx = create_connection(profile, **kwargs)
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
