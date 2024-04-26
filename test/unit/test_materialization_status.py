import json
from common_utils import QUERY_HISTORY_TASK_TABLE, WAREHOUSE_EVENTS_TASK_TABLE
from snowflake.connector import DictCursor
from datetime import datetime


# Schema for the materialization status view
TABLE_NAME = "TABLE_NAME"
SCHEMA_NAME = "SCHEMA_NAME"
LAST_FULL_START = "LAST_FULL_START"
LAST_FULL_DATA_START = "LAST_FULL_DATA_START"
LAST_FULL_DATA_END = "LAST_FULL_DATA_END"
LAST_FULL_STATUS = "LAST_FULL_STATUS"
LAST_FULL_ERROR_MESSAGE = "LAST_FULL_ERROR_MESSAGE"
LAST_FULL_QUERY_ID = "LAST_FULL_QUERY_ID"
LAST_INC_START = "LAST_INC_START"
LAST_INC_DATA_START = "LAST_INC_DATA_START"
LAST_INC_DATA_END = "LAST_INC_DATA_END"
LAST_INC_STATUS = "LAST_INC_STATUS"
LAST_INC_ERROR_MESSAGE = "LAST_INC_ERROR_MESSAGE"
LAST_INC_QUERY_ID = "LAST_INC_QUERY_ID"
NEXT_START = "NEXT_START"
NEXT_TYPE = "NEXT_TYPE"
NEXT_STATUS = "NEXT_STATUS"
NEXT_QUERY_ID = "NEXT_QUERY_ID"


def insert_row(
    cur, run: str, success: bool, input: dict, output: dict, table_name: str
):
    """
    Inserts a task status row into the given task status table.
    """
    input_json = "NULL"
    if input:
        input_json = f"parse_json('{json.dumps(input)}')"
    output_json = "NULL"
    if output:
        output_json = f"parse_json('{json.dumps(output)}')"
    cur.execute(
        f"""insert into {table_name} select '{run}'::TIMESTAMP, {success}, \
        {input_json}, {output_json}"""
    )


def verify_tables_returned(rows):
    assert len(rows) == 10
    assert [row[TABLE_NAME] for row in rows] == [
        "CLUSTER_SESSIONS",
        "ENRICHED_QUERY_HISTORY",
        "HYBRID_TABLE_USAGE_HISTORY",
        "LOGIN_HISTORY",
        "MATERIALIZED_VIEW_REFRESH_HISTORY",
        "SERVERLESS_TASK_HISTORY",
        "SESSIONS",
        "TASK_HISTORY",
        "WAREHOUSE_METERING_HISTORY",
        "WAREHOUSE_SESSIONS",
    ]
    assert [row[SCHEMA_NAME] for row in rows] == ["REPORTING"] * 10


def verify_row_contains(row: dict, expected: dict):
    for k, v in expected.items():
        assert k in row, f"Key {k} not found in row"
        if isinstance(row[k], datetime):
            assert row[k].strftime("%Y-%m-%d %H:%M:%S") == v
        else:
            assert row[k] == v


def test_initial_materialization_status(conn, reset_task_histories):
    with conn() as cnx:
        cur = cnx.cursor(DictCursor)

        rows = cur.execute(
            "select * from admin.materialization_status order by table_name"
        ).fetchall()

        verify_tables_returned(rows)

        for row in rows:
            # no full execution
            verify_row_contains(
                row,
                {
                    LAST_FULL_START: None,
                    LAST_FULL_DATA_START: None,
                    LAST_FULL_DATA_END: None,
                    LAST_FULL_STATUS: None,
                    LAST_FULL_ERROR_MESSAGE: None,
                    LAST_FULL_QUERY_ID: None,
                },
            )

            # no incremental execution
            verify_row_contains(
                row,
                {
                    LAST_INC_START: None,
                    LAST_INC_DATA_START: None,
                    LAST_INC_DATA_END: None,
                    LAST_INC_STATUS: None,
                    LAST_INC_ERROR_MESSAGE: None,
                    LAST_INC_QUERY_ID: None,
                },
            )

            # should have the next execution scheduled
            assert row[NEXT_TYPE] == "FULL"
            # The task is either executing or scheduled
            assert row[NEXT_STATUS] in ("SCHEDULED", "EXECUTING")
            if row[NEXT_STATUS] == "SCHEDULED":
                assert row[NEXT_START] is not None
                assert row[NEXT_QUERY_ID] is None
            else:
                assert row[NEXT_START] is not None
                assert row[NEXT_QUERY_ID] is not None


def test_full_materialization_status(conn, reset_task_histories):
    with conn() as cnx:
        cur = cnx.cursor(DictCursor)

        # Query History
        input = None
        output = {
            "new_INCOMPLETE": 100,
            "new_closed": 1000,
            "new_records": 1100,
            "newest_completed": "2022-04-01 00:45:00",
            "oldest_running": "2022-04-01 00:59:00",
            "materialized_start": "2022-04-01 00:00:00",
            "materialized_end": "2022-04-01 00:45:00",
        }
        insert_row(
            cur, "2022-04-01 03:00:00", True, input, output, QUERY_HISTORY_TASK_TABLE
        )

        # Warehouse Events
        input = None
        output = {
            "new_INCOMPLETE": 5,
            "new_closed": 100,
            "new_records": 105,
            "newest_completed": "2022-04-01 00:55:00",
            "oldest_running": "2022-04-01 00:55:00",
            "materialized_start": "2022-04-01 00:00:00",
            "materialized_end": "2022-04-01 00:55:00",
        }
        insert_row(
            cur, "2022-04-01 04:00:00", True, input, output, WAREHOUSE_EVENTS_TASK_TABLE
        )

        rows = cur.execute(
            "select * from admin.materialization_status order by table_name"
        ).fetchall()

        verify_tables_returned(rows)

        expected_qh = {
            LAST_FULL_START: "2022-04-01 03:00:00",
            LAST_FULL_DATA_START: "2022-04-01 00:00:00",
            LAST_FULL_DATA_END: "2022-04-01 00:45:00",
            LAST_FULL_STATUS: "SUCCESS",
            NEXT_TYPE: "INCREMENTAL",
        }
        row = next(row for row in rows if row[TABLE_NAME] == "ENRICHED_QUERY_HISTORY")
        verify_row_contains(row, expected_qh)
        assert row[NEXT_STATUS] in ("SCHEDULED", "EXECUTING")

        expected_weh = {
            LAST_FULL_START: "2022-04-01 04:00:00",
            LAST_FULL_DATA_START: "2022-04-01 00:00:00",
            LAST_FULL_DATA_END: "2022-04-01 00:55:00",
            LAST_FULL_STATUS: "SUCCESS",
            NEXT_TYPE: "INCREMENTAL",
        }
        row = next(row for row in rows if row[TABLE_NAME] == "WAREHOUSE_SESSIONS")
        verify_row_contains(row, expected_weh)
        assert row[NEXT_STATUS] in ("SCHEDULED", "EXECUTING")


def test_incremental_materialization_status(conn, reset_task_histories):
    with conn() as cnx:
        cur = cnx.cursor(DictCursor)

        # Query History
        input = None
        output = {
            "new_INCOMPLETE": 100,
            "new_closed": 1000,
            "new_records": 1100,
            "newest_completed": "2022-04-01 00:45:00",
            "oldest_running": "2022-04-01 00:59:00",
            "materialized_start": "2022-04-01 00:00:00",
            "materialized_end": "2022-04-01 00:45:00",
        }
        insert_row(
            cur, "2022-04-01 03:00:00", True, input, output, QUERY_HISTORY_TASK_TABLE
        )

        input = output
        output = {
            "new_INCOMPLETE": 100,
            "new_closed": 1000,
            "new_records": 1100,
            "newest_completed": "2022-04-01 01:45:00",
            "oldest_running": "2022-04-01 01:59:00",
            "materialized_start": "2022-04-01 00:45:00",
            "materialized_end": "2022-04-01 01:45:00",
        }
        insert_row(
            cur, "2022-04-01 05:00:00", True, input, output, QUERY_HISTORY_TASK_TABLE
        )

        # Warehouse Events
        input = None
        output = {
            "new_INCOMPLETE": 5,
            "new_closed": 100,
            "new_records": 105,
            "newest_completed": "2022-04-01 00:55:00",
            "oldest_running": "2022-04-01 00:55:00",
            "materialized_start": "2022-04-01 00:00:00",
            "materialized_end": "2022-04-01 00:55:00",
        }
        insert_row(
            cur, "2022-04-01 04:00:00", True, input, output, WAREHOUSE_EVENTS_TASK_TABLE
        )

        input = output
        output = {
            "new_INCOMPLETE": 5,
            "new_closed": 100,
            "new_records": 105,
            "newest_completed": "2022-04-01 01:55:00",
            "oldest_running": "2022-04-01 01:55:00",
            "materialized_start": "2022-04-01 00:55:00",
            "materialized_end": "2022-04-01 01:55:00",
        }
        insert_row(
            cur, "2022-04-01 06:00:00", True, input, output, WAREHOUSE_EVENTS_TASK_TABLE
        )

        rows = cur.execute(
            """select * from admin.materialization_status order by table_name"""
        ).fetchall()

        verify_tables_returned(rows)

        expected_qh = {
            LAST_FULL_START: "2022-04-01 03:00:00",
            LAST_FULL_DATA_START: "2022-04-01 00:00:00",
            LAST_FULL_DATA_END: "2022-04-01 00:45:00",
            LAST_FULL_STATUS: "SUCCESS",
            LAST_INC_START: "2022-04-01 05:00:00",
            LAST_INC_DATA_START: "2022-04-01 00:45:00",
            LAST_INC_DATA_END: "2022-04-01 01:45:00",
            LAST_INC_STATUS: "SUCCESS",
            NEXT_TYPE: "INCREMENTAL",
        }
        row = next(row for row in rows if row[TABLE_NAME] == "ENRICHED_QUERY_HISTORY")
        verify_row_contains(row, expected_qh)
        assert row[NEXT_STATUS] in ("SCHEDULED", "EXECUTING")

        expected_weh = {
            LAST_FULL_START: "2022-04-01 04:00:00",
            LAST_FULL_DATA_START: "2022-04-01 00:00:00",
            LAST_FULL_DATA_END: "2022-04-01 00:55:00",
            LAST_FULL_STATUS: "SUCCESS",
            LAST_INC_START: "2022-04-01 06:00:00",
            LAST_INC_DATA_START: "2022-04-01 00:55:00",
            LAST_INC_DATA_END: "2022-04-01 01:55:00",
            LAST_INC_STATUS: "SUCCESS",
            NEXT_TYPE: "INCREMENTAL",
        }
        row = next(row for row in rows if row[TABLE_NAME] == "WAREHOUSE_SESSIONS")
        verify_row_contains(row, expected_weh)
        assert row[NEXT_STATUS] in ("SCHEDULED", "EXECUTING")


def test_failed_full_materialization(conn, reset_task_histories):
    with conn() as cnx:
        cur = cnx.cursor(DictCursor)

        # Query History
        output = {
            "Error type": "Other error",
            "SQLCODE": 12345,
            "SQLERRM": "query history failure",
            "SQLSTATE": "A2345",
        }
        insert_row(
            cur, "2022-04-01 03:00:00", False, None, output, QUERY_HISTORY_TASK_TABLE
        )

        output["SQLERRM"] = "warehouse events failure"
        insert_row(
            cur,
            "2022-04-01 04:00:00",
            False,
            None,
            output,
            WAREHOUSE_EVENTS_TASK_TABLE,
        )

        rows = cur.execute(
            """select * from admin.materialization_status order by table_name"""
        ).fetchall()

        verify_tables_returned(rows)

        expected_weh = {
            LAST_FULL_START: "2022-04-01 04:00:00",
            LAST_FULL_STATUS: "FAILURE",
            LAST_FULL_ERROR_MESSAGE: "warehouse events failure",
            NEXT_TYPE: "FULL",
        }
        row = next(row for row in rows if row[TABLE_NAME] == "WAREHOUSE_SESSIONS")
        verify_row_contains(row, expected_weh)
        assert row[NEXT_STATUS] in ("SCHEDULED", "EXECUTING")

        expected_qh = {
            LAST_FULL_START: "2022-04-01 03:00:00",
            LAST_FULL_STATUS: "FAILURE",
            LAST_FULL_ERROR_MESSAGE: "query history failure",
            NEXT_TYPE: "FULL",
        }
        row = next(row for row in rows if row[TABLE_NAME] == "ENRICHED_QUERY_HISTORY")
        verify_row_contains(row, expected_qh)
        assert row[NEXT_STATUS] in ("SCHEDULED", "EXECUTING")


def test_failed_inc_materialization(conn, reset_task_histories):
    with conn() as cnx:
        cur = cnx.cursor(DictCursor)

        # Query History
        input = None
        output = {
            "new_INCOMPLETE": 100,
            "new_closed": 1000,
            "new_records": 1100,
            "newest_completed": "2022-04-01 00:45:00",
            "oldest_running": "2022-04-01 00:59:00",
            "materialized_start": "2022-04-01 00:00:00",
            "materialized_end": "2022-04-01 00:45:00",
        }
        insert_row(
            cur, "2022-04-01 03:00:00", True, input, output, QUERY_HISTORY_TASK_TABLE
        )

        input = output
        output = {
            "Error type": "Other error",
            "SQLCODE": 12345,
            "SQLERRM": "query history failure",
            "SQLSTATE": "A2345",
        }
        insert_row(
            cur, "2022-04-01 05:00:00", False, input, output, QUERY_HISTORY_TASK_TABLE
        )

        # Warehouse events
        input = None
        output = {
            "new_INCOMPLETE": 100,
            "new_closed": 1000,
            "new_records": 1100,
            "newest_completed": "2022-04-01 00:55:00",
            "oldest_running": "2022-04-01 00:59:00",
            "materialized_start": "2022-04-01 00:00:00",
            "materialized_end": "2022-04-01 00:55:00",
        }
        insert_row(
            cur, "2022-04-01 04:00:00", True, input, output, WAREHOUSE_EVENTS_TASK_TABLE
        )

        input = output
        output = {
            "Error type": "Other error",
            "SQLCODE": 12345,
            "SQLERRM": "warehouse events failure",
            "SQLSTATE": "A2345",
        }
        insert_row(
            cur,
            "2022-04-01 06:00:00",
            False,
            input,
            output,
            WAREHOUSE_EVENTS_TASK_TABLE,
        )

        rows = cur.execute(
            """select * from admin.materialization_status order by table_name"""
        ).fetchall()

        verify_tables_returned(rows)

        expected_qh = {
            LAST_FULL_START: "2022-04-01 03:00:00",
            LAST_FULL_DATA_START: "2022-04-01 00:00:00",
            LAST_FULL_DATA_END: "2022-04-01 00:45:00",
            LAST_FULL_STATUS: "SUCCESS",
            LAST_INC_START: "2022-04-01 05:00:00",
            LAST_INC_STATUS: "FAILURE",
            LAST_INC_ERROR_MESSAGE: "query history failure",
            NEXT_TYPE: "INCREMENTAL",
        }
        row = next(row for row in rows if row[TABLE_NAME] == "ENRICHED_QUERY_HISTORY")
        verify_row_contains(row, expected_qh)
        assert row[NEXT_STATUS] in ("SCHEDULED", "EXECUTING")

        expected_weh = {
            LAST_FULL_START: "2022-04-01 04:00:00",
            LAST_FULL_DATA_START: "2022-04-01 00:00:00",
            LAST_FULL_DATA_END: "2022-04-01 00:55:00",
            LAST_FULL_STATUS: "SUCCESS",
            LAST_INC_START: "2022-04-01 06:00:00",
            LAST_INC_STATUS: "FAILURE",
            LAST_INC_ERROR_MESSAGE: "warehouse events failure",
            NEXT_TYPE: "INCREMENTAL",
        }
        row = next(row for row in rows if row[TABLE_NAME] == "WAREHOUSE_SESSIONS")
        verify_row_contains(row, expected_weh)
        assert row[NEXT_STATUS] in ("SCHEDULED", "EXECUTING")
