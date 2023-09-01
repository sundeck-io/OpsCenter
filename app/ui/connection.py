import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.exceptions import SnowparkSessionException
from configparser import ConfigParser
import os
from threading import Lock
from cachetools import TTLCache, cached

# Add back once more things are supported in native apps.
# class Runner:
#     job: AsyncJob
#     is_select: bool
#
#     def __init__(self, job: AsyncJob, is_select: bool = False):
#         self.job = job
#         self.is_select = is_select
#
#     def result(self):
#         if self.is_select:
#             return self.job.result()
#         else:
#             rows = self.job.result()
#             return pd.DataFrame([row.as_dict() for row in rows])


class Connection:
    session: Session = None
    session_lock = Lock()
    cache = TTLCache(maxsize=100, ttl=60)

    @staticmethod
    def remove_quotes(string):
        if string.startswith('"'):
            string = string[1:]
        if string.endswith('"'):
            string = string[:-1]
        return string

    @classmethod
    @cached(cache)
    def cached(cls, sql: str):
        return execute(sql)

    @classmethod
    def get(cls):
        if cls.session is not None:
            return cls.session

        with cls.session_lock:
            if cls.session is None:
                try:
                    cls.session = get_active_session()
                except SnowparkSessionException:
                    # Assume local development env. Read config from snowsql.

                    # Define the path to the SnowSQL config file
                    config_path = os.path.expanduser("~/.snowsql/config")
                    profile_name = os.getenv("OPSCENTER_PROFILE", "opscenter")
                    section = f"connections.{profile_name}"

                    # Create a ConfigParser object and read the config file
                    config = ConfigParser()
                    config.read(config_path)

                    # Get the accountname, username, and password properties from the [connections] section
                    accountname = Connection.remove_quotes(
                        config.get(section, "accountname")
                    )
                    username = Connection.remove_quotes(config.get(section, "username"))
                    password = Connection.remove_quotes(config.get(section, "password"))
                    database = Connection.remove_quotes(config.get(section, "dbname"))
                    # Matches the default in deploy/devdeploy.py
                    schema = Connection.remove_quotes(
                        config.get(section, "schemaname", fallback="PUBLIC")
                    )

                    connection_parameters = {
                        "account": accountname,
                        "user": username,
                        "password": password,
                        "database": database,
                        "schema": schema,
                    }

                    cls.session = Session.builder.configs(connection_parameters).create()
            return cls.session

    @staticmethod
    def convert(v) -> str:
        if v is None:
            return "NULL"
        elif isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        elif isinstance(v, int):
            return str(v)
        elif isinstance(v, dict):
            return f"""object_construct({','.join(f"'{k}', {Connection.convert(value)}" for k, value in v.items())})"""
        elif isinstance(v, list):
            v = ",".join(Connection.convert(value) for value in v)
            return f"""array_construct({v})"""
        else:
            escaped = str(v).replace("'", "\\'")
            return f"'{escaped}'"

    # Hack until SNOW-796947 is fixed.
    # See: https://github.com/snowflakedb/snowpark-python/blob/182069e50e52028236532ec4eeab3fb86a758954/src/snowflake/snowpark/session.py#LL1205C16-L1205C28
    @staticmethod
    def bind(sql: str, args: dict):
        if args is None:
            return sql
        sanitized_dict = {}
        for k, v in args.items():
            sanitized_dict[k] = Connection.convert(v)
        return sql % sanitized_dict

    @classmethod
    def execute(cls, sql: str, args: dict = None, is_select: bool = False):
        sql = Connection.bind(sql, args)

        if is_select:
            # if asyncd:
            #    return Runner(cls.get().sql(sql).to_pandas(block=False), is_select)
            # else:
            # print("Executing (select): " + sql)
            return cls.get().sql(sql).to_pandas()
        else:
            # if asyncd:
            #    return Runner(cls.get().sql(sql).collect_nowait(), is_select)
            # else:
            # print("Executing (nonselect): " + sql)
            rows = cls.get().sql(sql).collect()
            return pd.DataFrame([row.as_dict() for row in rows])


def execute(sql: str, args: dict = None):
    return Connection.execute(sql, args)


def execute_select(sql: str, args: dict = None):
    return Connection.execute(sql, args, is_select=True)


# This isn't supported in native apps/snowpark :(
# def execute_async_select(sql: str, args: dict = None):
#    return Connection.execute(sql, args, is_select=True, asyncd=True)


def execute_with_cache(sql: str):
    return Connection.cached(sql)
