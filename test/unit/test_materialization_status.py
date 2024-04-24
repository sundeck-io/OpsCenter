import json


def recreate_task_history(cur):
    try:
        cur.execute("DROP VIEW IF EXISTS internal.all_task_history")
    except Exception as e:
        print(f"Ignoring exception during setup: {e}")
        pass
    cur.execute(
        "create or replace table internal.all_task_history(run timestamp, success boolean, "
        + "input object, output object, table_name text)"
    )


def insert_row(
    cur, run: str, success: bool, input: dict, output: dict, table_name: str
):
    input_json = "NULL"
    if input:
        input_json = f"parse_json('{json.dumps(input)}')"
    output_json = "NULL"
    if output:
        output_json = f"parse_json('{json.dumps(output)}')"
    cur.execute(
        f"""insert into internal.all_task_history select '{run}'::TIMESTAMP, {success}, \
        {input_json}, {output_json}, '{table_name}'"""
    )


def test_initial_materialization_status(conn):
    with conn() as cnx:
        cur = cnx.cursor()
        recreate_task_history(cur)

        rows = cur.execute(
            """select table_name, full_materialization_complete, last_execution, current_execution, next_execution from
            table(admin.materialization_status()) order by table_name"""
        ).fetchall()
        assert len(rows) == 2

        row = rows[0]
        assert row[0] == "QUERY_HISTORY"
        assert row[1] is False
        assert row[2] is None  # no last execution
        assert row[3] is None
        next_execution = json.loads(row[4])
        assert next_execution.get("kind", None) == "FULL"
        assert next_execution.get("estimated_start", None) is not None

        row = rows[1]
        assert row[0] == "WAREHOUSE_EVENTS_HISTORY"
        assert row[1] is False
        assert row[2] is None  # no last execution
        assert row[3] is None
        next_execution = json.loads(row[4])
        assert next_execution.get("kind", None) == "FULL"
        assert next_execution.get("estimated_start", None) is not None


def test_incremental_materialization_status(conn):
    with conn() as cnx:
        cur = cnx.cursor()
        recreate_task_history(cur)

        # Query History
        input = None
        output = {
            "new_INCOMPLETE": 100,
            "new_closed": 1000,
            "new_records": 1100,
            "newest_completed": "2022-04-01 00:45:00",
            "oldest_running": "2022-04-01 00:59:00",
        }
        insert_row(cur, "2022-04-01 04:00:00", True, input, output, "QUERY_HISTORY")

        input = output
        output = {
            "new_INCOMPLETE": 100,
            "new_closed": 1000,
            "new_records": 1100,
            "newest_completed": "2022-04-01 01:45:00",
            "oldest_running": "2022-04-01 01:59:00",
        }
        insert_row(cur, "2022-04-01 05:00:00", True, input, output, "QUERY_HISTORY")

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
            cur, "2022-04-01 04:00:00", True, input, output, "WAREHOUSE_EVENTS_HISTORY"
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
            cur, "2022-04-01 05:00:00", True, input, output, "WAREHOUSE_EVENTS_HISTORY"
        )

        rows = cur.execute(
            """select table_name, full_materialization_complete, last_execution, current_execution, next_execution from
            table(admin.materialization_status()) order by table_name"""
        ).fetchall()
        assert len(rows) == 2

        row = rows[0]
        assert row[0] == "QUERY_HISTORY"
        assert row[1] is True
        last_execution = json.loads(row[2])
        assert last_execution == {
            "kind": "INCREMENTAL",
            "success": True,
            "start": "2022-04-01 05:00:00.000 -0700",
        }
        assert row[3] is None
        next_execution = json.loads(row[4])
        assert next_execution == {
            "estimated_start": "2022-04-01 06:00:00.000 -0700",
            "kind": "INCREMENTAL",
        }

        row = rows[1]
        assert row[0] == "WAREHOUSE_EVENTS_HISTORY"
        assert row[1] is True
        last_execution = json.loads(row[2])
        assert last_execution == {
            "kind": "INCREMENTAL",
            "success": True,
            "start": "2022-04-01 05:00:00.000 -0700",
        }
        assert row[3] is None
        next_execution = json.loads(row[4])
        assert next_execution == {
            "estimated_start": "2022-04-01 06:00:00.000 -0700",
            "kind": "INCREMENTAL",
        }


def test_failed_full_materialization(conn):
    with conn() as cnx:
        cur = cnx.cursor()
        recreate_task_history(cur)

        # Query History
        input = None
        output = {
            "Error type": "Other error",
            "SQLCODE": 12345,
            "SQLERRM": "An error message",
            "SQLSTATE": "A2345",
        }
        insert_row(cur, "2022-04-01 04:00:00", False, input, output, "QUERY_HISTORY")
        insert_row(
            cur, "2022-04-01 04:00:00", False, input, output, "WAREHOUSE_EVENTS_HISTORY"
        )

        rows = cur.execute(
            """select table_name, full_materialization_complete, last_execution, current_execution, next_execution from
            table(admin.materialization_status()) order by table_name"""
        ).fetchall()
        assert len(rows) == 2

        def verify_row(row):
            assert row[1] is False
            last_execution = json.loads(row[2])
            assert last_execution == {
                "kind": "FULL",
                "success": False,
                "start": "2022-04-01 04:00:00.000 -0700",
                "error_message": "An error message",
            }
            assert row[3] is None
            next_execution = json.loads(row[4])
            assert next_execution.get("kind", None) == "FULL"
            assert next_execution.get("estimated_start", None) is not None

        assert rows[0][0] == "QUERY_HISTORY"
        verify_row(rows[0])

        assert rows[1][0] == "WAREHOUSE_EVENTS_HISTORY"
        verify_row(rows[1])


def test_failed_inc_materialization(conn):
    with conn() as cnx:
        cur = cnx.cursor()
        recreate_task_history(cur)

        # Query History
        input = None
        output = {
            "new_INCOMPLETE": 100,
            "new_closed": 1000,
            "new_records": 1100,
            "newest_completed": "2022-04-01 00:45:00",
            "oldest_running": "2022-04-01 00:59:00",
        }
        insert_row(cur, "2022-04-01 04:00:00", True, input, output, "QUERY_HISTORY")

        input = output
        output = {
            "Error type": "Other error",
            "SQLCODE": 12345,
            "SQLERRM": "An error message",
            "SQLSTATE": "A2345",
        }
        insert_row(cur, "2022-04-01 05:00:00", False, input, output, "QUERY_HISTORY")

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
            cur, "2022-04-01 04:00:00", True, input, output, "WAREHOUSE_EVENTS_HISTORY"
        )

        input = output
        output = {
            "Error type": "Other error",
            "SQLCODE": 12345,
            "SQLERRM": "An error message",
            "SQLSTATE": "A2345",
        }
        insert_row(
            cur, "2022-04-01 05:00:00", False, input, output, "WAREHOUSE_EVENTS_HISTORY"
        )

        rows = cur.execute(
            """select table_name, full_materialization_complete, last_execution, current_execution, next_execution from
            table(admin.materialization_status()) order by table_name"""
        ).fetchall()
        assert len(rows) == 2

        def verify_row(row):
            assert row[1] is False
            last_execution = json.loads(row[2])
            assert last_execution == {
                "kind": "INCREMENTAL",  # last failed execution was incremental
                "success": False,
                "start": "2022-04-01 05:00:00.000 -0700",
                "error_message": "An error message",
            }
            assert row[3] is None
            next_execution = json.loads(row[4])
            assert (
                next_execution.get("kind", None) == "INCREMENTAL"
            )  # next execution should also be incremental
            assert next_execution.get("estimated_start", None) is not None

        assert rows[0][0] == "QUERY_HISTORY"
        verify_row(rows[0])

        assert rows[1][0] == "WAREHOUSE_EVENTS_HISTORY"
        verify_row(rows[1])
