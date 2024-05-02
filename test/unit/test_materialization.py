import datetime
import json
from snowflake.connector.cursor import DictCursor

TIMESTAMP_PATTERN = "%Y-%m-%d %H:%M:%S.%f"


def assert_is_datetime(s: str):
    assert datetime.datetime.strptime(
        s, TIMESTAMP_PATTERN
    ), f"Expected value to be a datetime: {s}"


def test_query_history_migration(conn):
    with conn() as cnx, cnx.cursor() as cur:
        # Drop some columns
        _ = cur.execute(
            'ALTER TABLE "INTERNAL_REPORTING_MV"."QUERY_HISTORY_COMPLETE_AND_DAILY" DROP COLUMN IF EXISTS '
            + '"QUERY_HASH", "QUERY_HASH_VERSION", "QUERY_PARAMETERIZED_HASH", "QUERY_PARAMETERIZED_HASH_VERSION"'
        )
        # Re-add those columns, but in an unexpected order
        _ = cur.execute(
            "ALTER TABLE INTERNAL_REPORTING_MV.QUERY_HISTORY_COMPLETE_AND_DAILY ADD COLUMN "
            + '"QUERY_PARAMETERIZED_HASH" NUMBER, "QUERY_HASH" TEXT, "QUERY_PARAMETERIZED_HASH_VERSION" NUMBER, '
            + '"QUERY_HASH_VERSION" TEXT'
        )
        # The migration code should detect the columns in an unexpected order and correct the order.
        obj_as_str = cur.execute("CALL INTERNAL.MIGRATE_QUERIES()").fetchone()[0]
        obj = json.loads(obj_as_str)
        assert (
            "migrate1" in obj and "migrate2" in obj
        ), f"Got unexpected result from INTERNAL.MIGRATE_QUERIES(): {obj}"
        assert (
            '"QUERY_HASH" TEXT, "QUERY_HASH_VERSION" NUMBER, "QUERY_PARAMETERIZED_HASH" TEXT, "QUERY_PARAMETERIZED_HASH_VERSION" NUMBER'
            in obj["migrate1"]
        ), f"Got unexpected result from INTERNAL.MIGRATE_QUERIES(): {obj}"

        # Verify that a query over the resulting view is not broken.
        _ = cur.execute(
            "SELECT * FROM REPORTING.ENRICHED_QUERY_HISTORY LIMIT 0"
        ).fetchone()

        # Verify that the columns are in the correct order after running MIGRATE_QUERIES
        query = (
            "select column_name, data_type from information_schema.columns where table_schema = "
            + "'INTERNAL_REPORTING_MV' and table_name = 'QUERY_HISTORY_COMPLETE_AND_DAILY' order by ordinal_position desc limit 9;"
        )
        rows = cur.execute(query).fetchall()
        assert rows[-4][0] == "QUERY_PARAMETERIZED_HASH_VERSION"
        assert rows[-4][1] == "NUMBER"

        assert rows[-3][0] == "QUERY_PARAMETERIZED_HASH"
        assert rows[-3][1] == "TEXT"

        assert rows[-2][0] == "QUERY_HASH_VERSION"
        assert rows[-2][1] == "NUMBER"

        assert rows[-1][0] == "QUERY_HASH"
        assert rows[-1][1] == "TEXT"


def test_task_log(conn):
    with conn() as cnx, cnx.cursor(DictCursor) as cur:
        # Get the latest QH and WEH rows
        rows = cur.execute(
            """SELECT * FROM REPORTING.TASK_LOG_HISTORY
            WHERE TASK_NAME IN ('QUERY_HISTORY_MAINTENANCE', 'WAREHOUSE_EVENTS_MAINTENANCE')
            AND (task_name, task_start) in (
                SELECT task_name, max(task_start) FROM REPORTING.TASK_LOG_HISTORY
                GROUP BY task_name
            );
        """
        ).fetchall()

        # Do some basic verification over the two, make sure fields are filled in.
        for task_name in ["QUERY_HISTORY_MAINTENANCE", "WAREHOUSE_EVENTS_MAINTENANCE"]:
            task_log_row = next(row for row in rows if row["TASK_NAME"] == task_name)

            assert task_log_row["SUCCESS"] is True
            assert task_log_row["TASK_RUN_ID"] is not None
            assert task_log_row["QUERY_ID"] is not None
            assert task_log_row["TASK_START"] is not None
            assert task_log_row["OUTPUT"] is not None

            output = json.loads(task_log_row["OUTPUT"])
            assert "attempted_migrate" in output
            assert output["attempted_migrate"] is True
            assert "new_records" in output
            assert isinstance(output["new_records"], int)
            if task_name == "WAREHOUSE_EVENTS_MAINTENANCE":
                # WEH is different in that we extract two different kinds of data from the same task/source-view.
                fields = [
                    "cluster_range_min",
                    "cluster_range_max",
                    "warehouse_range_min",
                    "warehouse_range_max",
                ]
            else:
                fields = ["range_min", "range_max"]

            for f in fields:
                assert f in output, f"Expected field {f} in output: {output}"
                assert datetime.datetime.strptime(
                    output[f], TIMESTAMP_PATTERN
                ), f"Expected field {f} to be a datetime: {output[f]}"


def test_start_finish_task(conn):
    with conn() as cnx, cnx.cursor(DictCursor) as cur:
        task_name = "test_task"
        object_name = "test_object"
        cur.execute(
            f"DELETE FROM INTERNAL.TASK_LOG WHERE TASK_NAME = '{task_name}' AND OBJECT_NAME = '{object_name}'"
        )

        # internal.start_task(task_name text, object_name text, start_time text, task_run_id text, query_id text)
        task_start = "2021-01-01 00:00:00.000"
        run_id = "test_run_id"
        query_id = "test_query_id"
        row = cur.execute(
            f"CALL INTERNAL.START_TASK('test_task', 'test_object', '{task_start}', '{run_id}', '{query_id}')"
        ).fetchone()

        assert row["START_TASK"] is None, "Expected not to find an input to be None"

        rows = cur.execute(
            f"select * from internal.task_log where task_name = '{task_name}' and object_name = '{object_name}'"
        ).fetchall()
        assert len(rows) == 1

        assert rows[0]["TASK_START"].strftime(TIMESTAMP_PATTERN) == task_start
        assert rows[0]["TASK_RUN_ID"] == run_id
        assert rows[0]["QUERY_ID"] == query_id
        for f in [
            "TASK_FINISH",
            "INPUT",
            "OUTPUT",
            "SUCCESS",
            "RANGE_MIN",
            "RANGE_MAX",
        ]:
            assert rows[0][f] is None, f"Expected field {f} to be None: {rows[0][f]}"

        output = {
            "new_records": 100,
            "new_INCOMPLETE": 1,
            "new_closed": 99,
            "range_min": "2021-01-01 00:00:00",
            "range_max": "2021-12-31 23:59:00",
            "newest_completed": "2021-12-31 23:59:00",
            "oldest_running": "2021-12-31 23:45:00",
        }
        cur.execute(
            f"CALL INTERNAL.FINISH_TASK('test_task', 'test_object', '{task_start}', '{run_id}', '{json.dump(output)}')"
        ).fetchone()

        rows = cur.execute(
            f"select * from internal.task_log where task_name = '{task_name}' and object_name = '{object_name}'"
        ).fetchall()
        assert len(rows) == 1

        # Check that the old fields are still set
        assert rows[0]["TASK_START"].strftime(TIMESTAMP_PATTERN) == task_start
        assert rows[0]["TASK_RUN_ID"] == run_id
        assert rows[0]["QUERY_ID"] == query_id

        # And that we have new fields now.
        assert rows[0]["SUCCESS"] is True
        assert rows[0]["TASK_FINISH"] is not None
        assert_is_datetime(rows[0]["TASK_FINISH"])
        assert rows[0]["OUTPUT"] is not None
        actual_output = json.loads(rows[0]["OUTPUT"])
        assert (
            actual_output == output
        ), f"Expected output to be {output}, got {actual_output}"
