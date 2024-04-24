import json
from common_utils import QUERY_HISTORY_TASK_TABLE, WAREHOUSE_EVENTS_TASK_TABLE
from snowflake.connector import DictCursor


# Schema for the materialization status UDTF
TABLE_NAME = "TABLE_NAME"
FULL_MATERIALIZATION_COMPLETE = "FULL_MATERIALIZATION_COMPLETE"
RANGE_MIN = "RANGE_MIN"
RANGE_MAX = "RANGE_MAX"
LAST_EXECUTION = "LAST_EXECUTION"
CURRENT_EXECUTION = "CURRENT_EXECUTION"
NEXT_EXECUTION = "NEXT_EXECUTION"


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


def test_materialized_range(conn, reset_task_histories):
    with conn() as cnx:
        cur = cnx.cursor(DictCursor)

        # Generate full materialization records
        input = None
        output = {
            "new_INCOMPLETE": 100,
            "new_closed": 1000,
            "new_records": 1100,
            "newest_completed": "2022-04-01 00:45:00",
            "oldest_running": "2022-04-01 00:59:00",
        }
        insert_row(
            cur, "2022-04-01 04:00:00", True, input, output, QUERY_HISTORY_TASK_TABLE
        )

        # Warehouse Events
        input = None
        output = {
            "new_INCOMPLETE": 5,
            "new_closed": 100,
            "new_records": 105,
            "newest_completed": "2022-04-01 00:55:00",
            "oldest_running": "2022-04-01 00:55:00",
        }
        insert_row(
            cur, "2022-04-01 04:00:00", True, input, output, WAREHOUSE_EVENTS_TASK_TABLE
        )

        rows = cur.execute(
            "select * from table(admin.materialization_status()) order by table_name"
        ).fetchall()
        assert len(rows) == 2

        qh = rows[0]
        expected = cur.execute(
            "select min(start_time) as range_min, max(start_time) as range_max from reporting.enriched_query_history"
        ).fetchone()
        assert qh[RANGE_MIN] == expected[RANGE_MIN]
        assert qh[RANGE_MAX] == expected[RANGE_MAX]

        weh = rows[1]
        expected = cur.execute(
            "select min(session_start) as range_min, max(session_start) as range_max from reporting.warehouse_sessions"
        ).fetchone()
        assert weh[RANGE_MIN] == expected[RANGE_MIN]
        assert weh[RANGE_MAX] == expected[RANGE_MAX]


def test_initial_materialization_status(conn, reset_task_histories):
    with conn() as cnx:
        cur = cnx.cursor(DictCursor)

        rows = cur.execute(
            "select * from table(admin.materialization_status()) order by table_name"
        ).fetchall()
        assert len(rows) == 2

        row = rows[0]
        assert row[TABLE_NAME] == "QUERY_HISTORY"
        assert row[FULL_MATERIALIZATION_COMPLETE] is False
        assert row[LAST_EXECUTION] is None  # no last execution
        assert row[CURRENT_EXECUTION] is None
        next_execution = json.loads(row[NEXT_EXECUTION])
        assert next_execution.get("kind", None) == "FULL"
        assert next_execution.get("estimated_start", None) is not None

        row = rows[1]
        assert row[TABLE_NAME] == "WAREHOUSE_EVENTS_HISTORY"
        assert row[FULL_MATERIALIZATION_COMPLETE] is False
        assert row[LAST_EXECUTION] is None  # no last execution
        assert row[CURRENT_EXECUTION] is None
        next_execution = json.loads(row[NEXT_EXECUTION])
        assert next_execution.get("kind", None) == "FULL"
        assert next_execution.get("estimated_start", None) is not None


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
        }
        insert_row(
            cur, "2022-04-01 04:00:00", True, input, output, QUERY_HISTORY_TASK_TABLE
        )

        # Warehouse Events
        input = None
        output = {
            "new_INCOMPLETE": 5,
            "new_closed": 100,
            "new_records": 105,
            "newest_completed": "2022-04-01 00:55:00",
            "oldest_running": "2022-04-01 00:55:00",
        }
        insert_row(
            cur, "2022-04-01 04:00:00", True, input, output, WAREHOUSE_EVENTS_TASK_TABLE
        )

        rows = cur.execute(
            "select * from table(admin.materialization_status()) order by table_name"
        ).fetchall()
        assert len(rows) == 2

        row = rows[0]
        assert row[TABLE_NAME] == "QUERY_HISTORY"
        assert row[FULL_MATERIALIZATION_COMPLETE] is True
        last_execution = json.loads(row[LAST_EXECUTION])
        assert last_execution == {
            "kind": "FULL",
            "success": True,
            "start": "2022-04-01 04:00:00.000 -0700",
        }
        assert row[CURRENT_EXECUTION] is None
        next_execution = json.loads(row[NEXT_EXECUTION])
        assert next_execution == {
            "estimated_start": "2022-04-01 05:00:00.000 -0700",
            "kind": "INCREMENTAL",
        }

        row = rows[1]
        assert row[TABLE_NAME] == "WAREHOUSE_EVENTS_HISTORY"
        assert row[FULL_MATERIALIZATION_COMPLETE] is True
        last_execution = json.loads(row[LAST_EXECUTION])
        assert last_execution == {
            "kind": "FULL",
            "success": True,
            "start": "2022-04-01 04:00:00.000 -0700",
        }
        assert row[CURRENT_EXECUTION] is None
        next_execution = json.loads(row[NEXT_EXECUTION])
        assert next_execution == {
            "estimated_start": "2022-04-01 05:00:00.000 -0700",
            "kind": "INCREMENTAL",
        }


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
        }
        insert_row(
            cur, "2022-04-01 04:00:00", True, input, output, QUERY_HISTORY_TASK_TABLE
        )

        input = output
        output = {
            "new_INCOMPLETE": 100,
            "new_closed": 1000,
            "new_records": 1100,
            "newest_completed": "2022-04-01 01:45:00",
            "oldest_running": "2022-04-01 01:59:00",
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
        }
        insert_row(
            cur, "2022-04-01 05:00:00", True, input, output, WAREHOUSE_EVENTS_TASK_TABLE
        )

        rows = cur.execute(
            """select table_name, full_materialization_complete, last_execution, current_execution, next_execution from
            table(admin.materialization_status()) order by table_name"""
        ).fetchall()
        assert len(rows) == 2

        row = rows[0]
        assert row[TABLE_NAME] == "QUERY_HISTORY"
        assert row[FULL_MATERIALIZATION_COMPLETE] is True
        last_execution = json.loads(row[LAST_EXECUTION])
        assert last_execution == {
            "kind": "INCREMENTAL",
            "success": True,
            "start": "2022-04-01 05:00:00.000 -0700",
        }
        assert row[CURRENT_EXECUTION] is None
        next_execution = json.loads(row[NEXT_EXECUTION])
        assert next_execution == {
            "estimated_start": "2022-04-01 06:00:00.000 -0700",
            "kind": "INCREMENTAL",
        }

        row = rows[1]
        assert row[TABLE_NAME] == "WAREHOUSE_EVENTS_HISTORY"
        assert row[FULL_MATERIALIZATION_COMPLETE] is True
        last_execution = json.loads(row[LAST_EXECUTION])
        assert last_execution == {
            "kind": "INCREMENTAL",
            "success": True,
            "start": "2022-04-01 05:00:00.000 -0700",
        }
        assert row[CURRENT_EXECUTION] is None
        next_execution = json.loads(row[NEXT_EXECUTION])
        assert next_execution == {
            "estimated_start": "2022-04-01 06:00:00.000 -0700",
            "kind": "INCREMENTAL",
        }


def test_failed_full_materialization(conn, reset_task_histories):
    with conn() as cnx:
        cur = cnx.cursor(DictCursor)

        # Query History
        input = None
        output = {
            "Error type": "Other error",
            "SQLCODE": 12345,
            "SQLERRM": "An error message",
            "SQLSTATE": "A2345",
        }
        insert_row(
            cur, "2022-04-01 04:00:00", False, input, output, QUERY_HISTORY_TASK_TABLE
        )
        insert_row(
            cur,
            "2022-04-01 04:00:00",
            False,
            input,
            output,
            WAREHOUSE_EVENTS_TASK_TABLE,
        )

        rows = cur.execute(
            """select table_name, full_materialization_complete, last_execution, current_execution, next_execution from
            table(admin.materialization_status()) order by table_name"""
        ).fetchall()
        assert len(rows) == 2

        # Verify the last execution (full) failed and the next execution is full
        def verify_row(row):
            assert row[FULL_MATERIALIZATION_COMPLETE] is False
            last_execution = json.loads(row[LAST_EXECUTION])
            assert last_execution == {
                "kind": "FULL",
                "success": False,
                "start": "2022-04-01 04:00:00.000 -0700",
                "error_message": "An error message",
            }
            assert row[CURRENT_EXECUTION] is None
            next_execution = json.loads(row[NEXT_EXECUTION])
            assert next_execution.get("kind", None) == "FULL"
            assert next_execution.get("estimated_start", None) is not None

        assert rows[0][TABLE_NAME] == "QUERY_HISTORY"
        verify_row(rows[0])

        assert rows[1][TABLE_NAME] == "WAREHOUSE_EVENTS_HISTORY"
        verify_row(rows[1])


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
        }
        insert_row(
            cur, "2022-04-01 04:00:00", True, input, output, QUERY_HISTORY_TASK_TABLE
        )

        input = output
        output = {
            "Error type": "Other error",
            "SQLCODE": 12345,
            "SQLERRM": "An error message",
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
            "newest_completed": "2022-04-01 00:45:00",
            "oldest_running": "2022-04-01 00:59:00",
        }
        insert_row(
            cur, "2022-04-01 04:00:00", True, input, output, WAREHOUSE_EVENTS_TASK_TABLE
        )

        input = output
        output = {
            "Error type": "Other error",
            "SQLCODE": 12345,
            "SQLERRM": "An error message",
            "SQLSTATE": "A2345",
        }
        insert_row(
            cur,
            "2022-04-01 05:00:00",
            False,
            input,
            output,
            WAREHOUSE_EVENTS_TASK_TABLE,
        )

        rows = cur.execute(
            """select table_name, full_materialization_complete, last_execution, current_execution, next_execution from
            table(admin.materialization_status()) order by table_name"""
        ).fetchall()
        assert len(rows) == 2

        # Verify that the last execution (incremental) failed and the next execution is also incremental
        def verify_row(row):
            # The first materialization was successful which implies the full was completed
            assert row[FULL_MATERIALIZATION_COMPLETE] is True
            last_execution = json.loads(row[LAST_EXECUTION])
            assert last_execution == {
                "kind": "INCREMENTAL",  # last failed execution was incremental
                "success": False,
                "start": "2022-04-01 05:00:00.000 -0700",
                "error_message": "An error message",
            }
            assert row[CURRENT_EXECUTION] is None
            next_execution = json.loads(row[NEXT_EXECUTION])
            assert (
                next_execution.get("kind", None) == "INCREMENTAL"
            )  # next execution should also be incremental
            assert next_execution.get("estimated_start", None) is not None

        assert rows[0][TABLE_NAME] == "QUERY_HISTORY"
        verify_row(rows[0])

        assert rows[1][TABLE_NAME] == "WAREHOUSE_EVENTS_HISTORY"
        verify_row(rows[1])
