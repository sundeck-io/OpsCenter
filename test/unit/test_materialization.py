import datetime
import json
import time

from snowflake.connector.cursor import DictCursor
import unittest

TIMESTAMP_PATTERN = "%Y-%m-%d %H:%M:%S.%f %Z"


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
            """select * FROM ADMIN.TASK_LOG_HISTORY
                where success is not null and TASK_NAME IN ('QUERY_HISTORY_MAINTENANCE', 'WAREHOUSE_EVENTS_MAINTENANCE')
                QUALIFY ROW_NUMBER() OVER (PARTITION BY task_name, object_name ORDER BY task_start DESC) = 1;
        """
        ).fetchall()

        # Do some basic verification over the two, make sure fields are filled in.
        for task_log_row in rows:
            print(f"Row: {task_log_row}")
            task_name = task_log_row["TASK_NAME"]

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
                # in the pre-commit account, the little amount of data combined with the small materialization
                # range means we can have no warehouse_session rows.
                assert output[f] is None or datetime.datetime.strptime(
                    output[f], "%Y-%m-%d %H:%M:%S.%f"
                ), f"Expected field {f} to be a nullable datetime: {output[f]}"


def test_start_finish_task(conn):
    with conn() as cnx, cnx.cursor(DictCursor) as cur:
        task_name = "test_task"
        object_name = "test_object"
        cur.execute(
            f"DELETE FROM INTERNAL.TASK_LOG WHERE TASK_NAME = '{task_name}' AND OBJECT_NAME = '{object_name}'"
        )

        # internal.start_task(task_name text, object_name text, task_run_id text, query_id text)
        run_id = "test_run_id"
        query_id = "test_query_id"
        input = cur.execute(
            f"CALL INTERNAL.START_TASK('test_task', 'test_object', '{run_id}', '{query_id}')"
        ).fetchone()

        assert input["START_TASK"] is None, "Expected NULL from internal.start_task"

        rows = cur.execute(
            f"select * from internal.task_log where task_name = '{task_name}' and object_name = '{object_name}'"
        ).fetchall()
        assert len(rows) == 1

        assert rows[0]["TASK_START"] is not None, "Should have a start time of the task"
        orig_task_start = rows[0]["TASK_START"]
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
            f"CALL INTERNAL.FINISH_TASK('{task_name}', '{object_name}', '{run_id}', PARSE_JSON('{json.dumps(output)}'))"
        ).fetchone()

        rows = cur.execute(
            f"select * from internal.task_log where task_name = '{task_name}' and object_name = '{object_name}'"
        ).fetchall()
        assert len(rows) == 1

        # Check that the old fields are still set
        assert rows[0]["TASK_START"] == orig_task_start
        assert rows[0]["TASK_RUN_ID"] == run_id
        assert rows[0]["QUERY_ID"] == query_id

        # And that we have new fields now.
        assert rows[0]["SUCCESS"] is True
        assert rows[0]["TASK_FINISH"] is not None
        assert isinstance(rows[0]["TASK_FINISH"], datetime.datetime)
        assert rows[0]["OUTPUT"] is not None
        actual_output = json.loads(rows[0]["OUTPUT"])
        assert (
            actual_output == output
        ), f"Expected output to be {output}, got {actual_output}"


def test_materialization_status_structure(conn):
    tc = unittest.TestCase()
    with conn() as cnx, cnx.cursor(DictCursor) as cur:
        # Schema
        expected_columns = [
            "USER_SCHEMA",
            "USER_VIEW",
            "RANGE_START",
            "RANGE_END",
            "PARTITION",
            "LAST_FULL_START",
            "LAST_FULL_END",
            "LAST_FULL_STATUS",
            "LAST_FULL_ERROR_MESSAGE",
            "LAST_FULL_QUERY_ID",
            "LAST_INCR_START",
            "LAST_INCR_END",
            "LAST_INCR_STATUS",
            "LAST_INCR_ERROR_MESSAGE",
            "LAST_INCR_QUERY_ID",
            "NEXT_START",
            "NEXT_TYPE",
            "NEXT_STATUS",
            "NEXT_QUERY_ID",
        ]

        # Tests that the two lists have the same elements, irrespective of order
        cur.execute("SELECT * FROM ADMIN.MATERIALIZATION_STATUS LIMIT 0")
        tc.assertCountEqual(
            [col.name for col in cur.description],
            expected_columns,
            "Schema of admin.materialization_status view is incorrect.",
        )

        rows = cur.execute("SHOW VIEWS IN SCHEMA REPORTING").fetchall()

        ignored_views = [
            "QUERY_MONITOR_ACTIVITY",
            "QUOTA_TASK_HISTORY",
            "SUNDECK_QUERY_HISTORY",
            "TASK_LOG_HISTORY",
            "UPGRADE_HISTORY",
            "WAREHOUSE_SCHEDULES_TASK_HISTORY",
            "WAREHOUSE_LOAD_EVENTS_TASK_HISTORY",
            "SIMPLE_DATA_EVENTS_TASK_HISTORY",
        ]

        # Subtract views we don't include in materialization_status
        reporting_views = [
            row["name"] for row in rows if row["name"] not in ignored_views
        ]

        # Check for the expected set of rows
        rows = cur.execute(
            "select distinct user_view from admin.materialization_status"
        ).fetchall()

        for row in rows:
            assert (
                row["USER_VIEW"] in reporting_views
            ), f"Unexpected view in admin.materialization_status: {row['TABLE_NAME']}, expected views were {reporting_views}"

        assert len(rows) == len(
            reporting_views
        ), f"Expected equal number of reporting views as rows in admin.materialization_status. Reporting views {reporting_views}, materialization_status rows {rows}"


def test_materialization_status_values(conn):
    with conn() as cnx, cnx.cursor(DictCursor) as cur:
        rows = cur.execute(
            "select user_schema, user_view, partition, last_full_start, last_full_end, last_full_status, range_start, range_end from admin.materialization_status"
        ).fetchall()

        for row in rows:
            assert row["USER_SCHEMA"] is not None
            assert row["USER_VIEW"] is not None
            if row["USER_VIEW"] == "WAREHOUSE_LOAD_HISTORY":
                assert row["PARTITION"] is not None

            assert row["LAST_FULL_STATUS"] == "SUCCESS"
            for c in ["LAST_FULL_START", "LAST_FULL_END"]:
                assert row[c] is not None, f"Expected field {c} to be not null: {row}"
                assert isinstance(row[c], datetime.datetime)
            # If we have a range of data, the start should always be less than or equal to the end.
            if row["RANGE_START"] and row["RANGE_END"]:
                assert (
                    row["RANGE_START"] <= row["RANGE_END"]
                ), "Range end should never be less than range start"


def test_migrate_old_query_history_log(conn):
    with conn() as cnx, cnx.cursor(DictCursor) as cur:
        try:
            cur.execute(
                "delete from internal.config where key = 'MIGRATION_TASK_QUERY_HISTORY'"
            )
            cur.execute(
                "CREATE OR REPLACE TABLE INTERNAL.TASK_QUERY_HISTORY(run timestamp_ntz, success boolean, input variant, output variant)"
            )

            cur.execute(
                "INSERT INTO INTERNAL.TASK_QUERY_HISTORY select '2020-01-01 12:00:00'::TIMESTAMP_NTZ, TRUE, OBJECT_CONSTRUCT('input', true), OBJECT_CONSTRUCT('output', true)"
            )

            key = "QUERY_HISTORY_MAINTENANCE"
            last_task_run = cur.execute(
                f"select value from catalog.config where key = '{key}'"
            ).fetchone()["VALUE"]

            cur.execute("execute task tasks.query_history_maintenance")

            new_task_run = None
            # wait 2 minutes total, checking every 20 seconds
            for i in range(0, 6):
                time.sleep(20)

                new_task_run = cur.execute(
                    f"select value from catalog.config where key = '{key}'"
                ).fetchone()["VALUE"]
                if new_task_run != last_task_run:
                    break

            assert new_task_run is not None
            assert new_task_run != last_task_run, f"{key} task has not re-run"

            row = cur.execute(
                f"select * from internal.task_log where task_name = '{key}' and task_start <= '2020-01-01 12:00:00'::TIMESTAMP_NTZ"
            ).fetchone()

            assert row is not None
            assert row["SUCCESS"] is True
            assert row["INPUT"] is not None
            assert row["OUTPUT"] is not None
            input = json.loads(row["INPUT"])
            assert input == {"input": True}
            output = json.loads(row["OUTPUT"])
            assert output == {"output": True}
            assert row["TASK_START"] == datetime.datetime(2020, 1, 1, 12, 0, 0)
            assert row["TASK_FINISH"] == datetime.datetime(2020, 1, 1, 12, 0, 0)
            assert row["TASK_NAME"] == key
            assert row["OBJECT_NAME"] == "QUERY_HISTORY"
        finally:
            # Remove any junk rows we created
            cur.execute(
                "DELETE FROM INTERNAL.TASK_LOG where task_name = 'QUERY_HISTORY_MAINTENANCE' and task_start::DATE <= '2020-01-30'::DATE"
            )


def test_migrate_old_warehouse_events_log(conn):
    with conn() as cnx, cnx.cursor(DictCursor) as cur:
        try:
            cur.execute(
                "delete from internal.config where key = 'MIGRATION_TASK_WAREHOUSE_EVENTS'"
            )
            cur.execute(
                "CREATE OR REPLACE TABLE INTERNAL.TASK_WAREHOUSE_EVENTS(run timestamp_ntz, success boolean, input variant, output variant)"
            )

            cur.execute(
                "INSERT INTO INTERNAL.TASK_WAREHOUSE_EVENTS select '2020-01-01 12:00:00'::TIMESTAMP_NTZ, TRUE, OBJECT_CONSTRUCT('input', true), OBJECT_CONSTRUCT('output', true)"
            )

            key = "WAREHOUSE_EVENTS_MAINTENANCE"
            last_task_run = cur.execute(
                f"select value from catalog.config where key = '{key}'"
            ).fetchone()["VALUE"]

            cur.execute("execute task tasks.warehouse_events_maintenance")

            new_task_run = None
            # wait 2 minutes total, checking every 20 seconds
            for i in range(0, 6):
                time.sleep(20)

                new_task_run = cur.execute(
                    f"select value from catalog.config where key = '{key}'"
                ).fetchone()["VALUE"]
                if new_task_run != last_task_run:
                    break

            assert new_task_run is not None
            assert new_task_run != last_task_run, f"{key} task has not re-run"

            for obj in [
                "WAREHOUSE_EVENTS_HISTORY",
                "CLUSTER_SESSIONS",
                "WAREHOUSE_SESSIONS",
            ]:
                row = cur.execute(
                    f"select * from internal.task_log where task_name = '{key}' and task_start <= '2020-01-01 12:00:00'::TIMESTAMP_NTZ and object_name = '{obj}'"
                ).fetchone()

                assert row is not None
                assert row["SUCCESS"] is True
                assert row["INPUT"] is not None
                assert row["OUTPUT"] is not None
                input = json.loads(row["INPUT"])
                assert input == {"input": True}
                output = json.loads(row["OUTPUT"])
                assert output == {"output": True}
                assert row["TASK_START"] == datetime.datetime(2020, 1, 1, 12, 0, 0)
                assert row["TASK_FINISH"] == datetime.datetime(2020, 1, 1, 12, 0, 0)

                assert row["TASK_NAME"] == "WAREHOUSE_EVENTS_MAINTENANCE"
                assert row["OBJECT_NAME"] == obj
        finally:
            # Remove any junk rows we created
            cur.execute(
                "DELETE FROM INTERNAL.TASK_LOG where task_name = 'WAREHOUSE_EVENTS_MAINTENANCE' and task_start::DATE <= '2020-01-30'::DATE"
            )
