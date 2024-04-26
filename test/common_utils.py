import uuid
from typing import List, Dict


def generate_unique_name(prefix, timestamp_string) -> str:
    name = prefix + "_" + str(uuid.uuid4()) + "_" + timestamp_string
    return name


def _get_single_value(conn, sql):
    with conn() as cnx:
        cur = cnx.cursor()
        result = cur.execute(sql).fetchone()
        return result[0]


def run_proc(conn, sql) -> str:
    assert isinstance(result := _get_single_value(conn, sql), (str, type(None)))
    return result


def row_count(conn, sql) -> int:
    assert isinstance(result := _get_single_value(conn, sql), int)
    return result


def run_sql(conn, sql) -> str:
    result = _get_single_value(conn, sql)
    return str(result)


def delete_list_of_labels(conn, sql):
    print(f"[INFO] SQL in delete function: {sql}")

    with conn() as cnx:
        cur = cnx.cursor()
        for name in cur.execute(sql).fetchall():
            delete_label_statement = f"call ADMIN.DELETE_LABEL('{name[0]}');"
            assert (
                run_proc(conn, delete_label_statement) is None
            ), "Stored procedure output does not match expected result!"

        # Also try to delete a label with an empty name which may be there from the current test run
        _ = run_proc(conn, "call ADMIN.DELETE_LABEL('')")


def delete_list_of_probes(conn, sql):
    print(f"[INFO] SQL in delete function: {sql}")

    with conn() as cnx:
        cur = cnx.cursor()
        for name in cur.execute(sql).fetchall():
            delete_probe_statement = f"call ADMIN.DELETE_PROBE('{name[0]}');"
            assert run_proc(conn, delete_probe_statement) is None


def fetch_all_warehouse_schedules(conn) -> List[Dict]:
    with conn() as cnx, cnx.cursor() as cur:
        return cur.execute(
            "select * from internal.wh_schedules order by name, weekday, start_at"
        ).fetchall()


def reset_timezone(conn):
    with conn.cursor() as cur:
        _ = cur.execute(
            "call internal.set_config('default_timezone', 'America/Los_Angeles')"
        ).fetchone()


QUERY_HISTORY_TASK_TABLE = "internal.task_query_history"
WAREHOUSE_EVENTS_TASK_TABLE = "internal.task_warehouse_events"
SIMPLE_DATA_EVENTS_TASK_TABLE = "internal.task_simple_data_events"

SIMPLE_DATA_EVENT_TABLES = [
    "HYBRID_TABLE_USAGE_HISTORY",
    "LOGIN_HISTORY",
    "MATERIALIZED_VIEW_REFRESH_HISTORY",
    "SERVERLESS_TASK_HISTORY",
    "SESSIONS",
    "TASK_HISTORY",
    "WAREHOUSE_METERING_HISTORY",
]


def get_task_history_tables() -> List[str]:
    return [QUERY_HISTORY_TASK_TABLE, WAREHOUSE_EVENTS_TASK_TABLE]
