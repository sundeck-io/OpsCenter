from __future__ import annotations

import json
import uuid
from typing import List
from common_utils import generate_unique_name
from snowflake.connector import SnowflakeConnection


def _create_warehouse_for_test(
    conn: SnowflakeConnection, name: str, initial_size: str = "XSMALL"
):
    """
    Creates the warehouse we will try to test schedules against. We want to avoid using this warehouse for any tests,
    so we cordon it off on its own connection (else the DML will cause the current connection to use that WH).
    :param conn: A connection to Snowflake.
    :param name: The name of the warehouse to create.
    :param initial_size: The size of the warehouse to create.
    """
    # Create the warehouse on its own connection to avoid using that warehouse for the test
    with conn.cursor() as cur:
        _ = cur.execute(
            f"CREATE OR REPLACE WAREHOUSE {name} WITH WAREHOUSE_SIZE = {initial_size}"
        ).fetchone()


def _assert_no_updates(row: List):
    obj = json.loads(row[0])
    assert "num_candidates" in obj
    assert obj["num_candidates"] == 0, f"Expected no schedules to match: {obj}"
    assert "warehouses_updated" in obj
    assert (
        obj["warehouses_updated"] == 0
    ), f"Expected no warehouses to be updated: {obj}"


def _ensure_tables_created(conn):
    """
    Create the tables that are normally created by admin.finalize_setup() and clear out any old state.
    """
    with conn.cursor() as cur:
        # Ease local dev if we have run materialization
        _ = cur.execute(
            "call internal_python.create_table('WAREHOUSE_SCHEDULES')"
        ).fetchone()
        _ = cur.execute(
            "call internal_python.create_table('WAREHOUSE_ALTER_STATEMENTS')"
        ).fetchone()
        _ = cur.execute("truncate table internal.WH_SCHEDULES").fetchone()


def _update_warehouse_schedules_sql(last_run: str, now: str) -> str:
    # Assume the default account timezeone is "America/Los_Angeles". Due to weirdness with TIMESTAMP_LTZ
    # through a procedure, these are shifted backwards from UTC to Los_Angeles because the procedure shifts
    # them forward again. Probably something we don't understand...

    return f"""call INTERNAL.UPDATE_WAREHOUSE_SCHEDULES(
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', to_timestamp_ltz('{last_run}')),
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', to_timestamp_ltz('{now}')));"""


def test_basic_warehouse_schedule(conn, timestamp_string):
    wh_name = generate_unique_name("wh", timestamp_string).replace("-", "_")
    try:
        with conn() as cnx:
            _create_warehouse_for_test(cnx, wh_name)

        with conn() as cnx, cnx.cursor() as cur:
            # Set up internal state normally handled in admin.finalize_setup()
            _ensure_tables_created(cnx)

            id1 = uuid.uuid4().hex
            id2 = uuid.uuid4().hex

            # TODO Write and use crud-backed admin procedures. These procedures would generate the alter statement for us.
            sql = f"""INSERT INTO INTERNAL.WH_SCHEDULES select '{id1}', '{wh_name}', '00:00:00', '12:00:00',
                'X-Small', 1, TRUE, 0, 0, 'Standard', NULL, TRUE, NULL, TRUE"""
            _ = cur.execute(sql).fetchone()

            sql = f"""INSERT INTO INTERNAL.WH_SCHEDULES select '{id2}', '{wh_name}', '12:00:00', '23:59:00',
                'Small', 5, TRUE, 0, 0, 'Standard', NULL, TRUE, NULL, TRUE"""
            _ = cur.execute(sql).fetchone()

            sql = (
                f"INSERT INTO INTERNAL.WAREHOUSE_ALTER_STATEMENTS SELECT '{id1}', 'alter warehouse {wh_name} "
                + " set WAREHOUSE_SIZE = XSMALL, WAREHOUSE_TYPE = STANDARD, AUTO_SUSPEND = 1, AUTO_RESUME = True'"
            )
            _ = cur.execute(sql).fetchone()

            sql = (
                f"INSERT INTO INTERNAL.WAREHOUSE_ALTER_STATEMENTS SELECT '{id2}', 'alter warehouse {wh_name} "
                + " set WAREHOUSE_SIZE = SMALL, WAREHOUSE_TYPE = STANDARD, AUTO_SUSPEND = 1, AUTO_RESUME = True'"
            )
            _ = cur.execute(sql).fetchone()

            # Should do nothing be we have no new schedule
            row = cur.execute(
                _update_warehouse_schedules_sql(
                    "2023-09-29 10:00:00", "2023-09-29 10:15:00"
                )
            ).fetchone()
            _assert_no_updates(row)

            row = cur.execute(
                _update_warehouse_schedules_sql(
                    "2023-09-29 11:45:00", "2023-09-29 12:00:00"
                )
            ).fetchone()

            obj = json.loads(row[0])
            assert "warehouses_updated" in obj
            assert obj["warehouses_updated"] == 1
            assert "num_candidates" in obj
            assert obj["num_candidates"] == 1
            assert "statements" in obj
            assert len(obj["statements"]) == 1
            assert "WAREHOUSE_SIZE = SMALL" in obj["statements"][0]
    finally:
        with conn() as cnx, cnx.cursor() as cur:
            _ = cur.execute(f"DROP WAREHOUSE IF EXISTS {wh_name}").fetchone()


def test_alternate_timezone(conn, timestamp_string):
    wh_name = generate_unique_name("wh", timestamp_string).replace("-", "_")
    try:
        with conn() as cnx:
            _create_warehouse_for_test(cnx, wh_name)

        with conn() as cnx, cnx.cursor() as cur:
            # The Snowflake Account's timezone is America/Los_Angeles (-0700), but we are configured OpsCenter to
            # apply schedules as if they were in America/New_York (-0400).
            _ = cur.execute(
                "call internal.set_config('default_timezone', 'America/New_York')"
            ).fetchone()

            # Set up internal state normally handled in admin.finalize_setup()
            _ensure_tables_created(cnx)

            id1 = uuid.uuid4().hex
            id2 = uuid.uuid4().hex

            # TODO Write and use crud-backed admin procedures. These procedures would generate the alter statement for us.
            sql = f"""INSERT INTO INTERNAL.WH_SCHEDULES select '{id1}', '{wh_name}', '00:00:00', '12:00:00',
                'X-Small', 1, TRUE, 0, 0, 'Standard', NULL, TRUE, NULL, TRUE"""
            _ = cur.execute(sql).fetchone()

            sql = f"""INSERT INTO INTERNAL.WH_SCHEDULES select '{id2}', '{wh_name}', '12:00:00', '23:59:00',
                'Small', 5, TRUE, 0, 0, 'Standard', NULL, TRUE, NULL, TRUE"""
            _ = cur.execute(sql).fetchone()

            sql = (
                f"INSERT INTO INTERNAL.WAREHOUSE_ALTER_STATEMENTS SELECT '{id1}', 'alter warehouse {wh_name} "
                + " set WAREHOUSE_SIZE = XSMALL, WAREHOUSE_TYPE = STANDARD, AUTO_SUSPEND = 1, AUTO_RESUME = True'"
            )
            _ = cur.execute(sql).fetchone()

            sql = (
                f"INSERT INTO INTERNAL.WAREHOUSE_ALTER_STATEMENTS SELECT '{id2}', 'alter warehouse {wh_name} "
                + " set WAREHOUSE_SIZE = SMALL, WAREHOUSE_TYPE = STANDARD, AUTO_SUSPEND = 1, AUTO_RESUME = True'"
            )
            _ = cur.execute(sql).fetchone()

            # Assume the default account timezeone is "America/Los_Angeles". Due to weirdness with TIMESTAMP_LTZ
            # through a procedure, these are shifted backwards from UTC to Los_Angeles because the procedure shifts
            # them forward again. Probably something we don't understand...

            # These times are in UTC-7 (based on when the task runs), but need to be applied to the schedules in UTC-4
            row = cur.execute(
                _update_warehouse_schedules_sql(
                    "2023-09-29 07:45:00", "2023-09-29 08:00:00"
                )
            ).fetchone()
            _assert_no_updates(row)

            row = cur.execute(
                _update_warehouse_schedules_sql(
                    "2023-09-29 08:45:00", "2023-09-29 09:00:00"
                )
            ).fetchone()

            obj = json.loads(row[0])
            assert "warehouses_updated" in obj
            assert obj["warehouses_updated"] == 1
            assert "num_candidates" in obj
            assert obj["num_candidates"] == 1
            assert "statements" in obj
            assert len(obj["statements"]) == 1
            assert "WAREHOUSE_SIZE = SMALL" in obj["statements"][0]

            # This is actually last_run=14:25 and now=15:00 in UTC-4, so they should do nothing.
            row = cur.execute(
                _update_warehouse_schedules_sql(
                    "2023-09-29 11:45:00", "2023-09-29 12:00:00"
                )
            ).fetchone()
            _assert_no_updates(row)
    finally:
        with conn() as cnx, cnx.cursor() as cur:
            _ = cur.execute(f"DROP WAREHOUSE IF EXISTS {wh_name}").fetchone()


def test_from_london(conn, timestamp_string):
    wh_name = generate_unique_name("wh", timestamp_string).replace("-", "_")
    try:
        with conn() as cnx:
            _create_warehouse_for_test(cnx, wh_name)

        with conn() as cnx, cnx.cursor() as cur:
            # The Snowflake Account's timezone is America/Los_Angeles (-0700), but we are configured OpsCenter to
            # apply schedules as if they were in Europe/London (+0100).
            _ = cur.execute(
                "call internal.set_config('default_timezone', 'Europe/London')"
            ).fetchone()

            # Set up internal state normally handled in admin.finalize_setup()
            _ensure_tables_created(cnx)

            (id1, id2, id3, id4, id5) = [uuid.uuid4().hex for x in range(1, 6)]

            # TODO Write and use crud-backed admin procedures. These procedures would generate the alter statement for us.
            sql = f"""INSERT INTO INTERNAL.WH_SCHEDULES select '{id1}', '{wh_name}', '00:00:00', '09:00:00',
                'X-Small', 1, TRUE, 0, 0, 'Standard', NULL, TRUE, NULL, TRUE"""
            _ = cur.execute(sql).fetchone()

            sql = f"""INSERT INTO INTERNAL.WH_SCHEDULES select '{id2}', '{wh_name}', '09:00:00', '17:00:00',
                'Medium', 15, TRUE, 0, 0, 'Standard', NULL, TRUE, NULL, TRUE"""
            _ = cur.execute(sql).fetchone()

            sql = f"""INSERT INTO INTERNAL.WH_SCHEDULES select '{id3}', '{wh_name}', '17:00:00', '23:59:00',
                'Small', 5, TRUE, 0, 0, 'Standard', NULL, TRUE, NULL, TRUE"""
            _ = cur.execute(sql).fetchone()

            sql = (
                f"INSERT INTO INTERNAL.WAREHOUSE_ALTER_STATEMENTS SELECT '{id1}', 'alter warehouse {wh_name} "
                + " set WAREHOUSE_SIZE = XSMALL, WAREHOUSE_TYPE = STANDARD, AUTO_SUSPEND = 1, AUTO_RESUME = True'"
            )
            _ = cur.execute(sql).fetchone()

            sql = (
                f"INSERT INTO INTERNAL.WAREHOUSE_ALTER_STATEMENTS SELECT '{id2}', 'alter warehouse {wh_name} "
                + " set WAREHOUSE_SIZE = MEDIUM, WAREHOUSE_TYPE = STANDARD, AUTO_SUSPEND = 15, AUTO_RESUME = True'"
            )
            _ = cur.execute(sql).fetchone()

            sql = (
                f"INSERT INTO INTERNAL.WAREHOUSE_ALTER_STATEMENTS SELECT '{id3}', 'alter warehouse {wh_name} "
                + " set WAREHOUSE_SIZE = SMALL, WAREHOUSE_TYPE = STANDARD, AUTO_SUSPEND = 5, AUTO_RESUME = True'"
            )
            _ = cur.execute(sql).fetchone()

            # Weekend nights run ETL
            sql = f"""INSERT INTO INTERNAL.WH_SCHEDULES select '{id4}', '{wh_name}', '00:00:00', '22:00:00',
                'X-Small', 0, TRUE, 0, 0, 'Standard', NULL, FALSE, NULL, TRUE"""
            _ = cur.execute(sql).fetchone()

            sql = f"""INSERT INTO INTERNAL.WH_SCHEDULES select '{id5}', '{wh_name}', '22:00:00', '23:59:00',
                '2X-Large', 15, TRUE, 0, 0, 'Standard', NULL, FALSE, NULL, TRUE"""
            _ = cur.execute(sql).fetchone()

            sql = (
                f"INSERT INTO INTERNAL.WAREHOUSE_ALTER_STATEMENTS SELECT '{id4}', 'alter warehouse {wh_name} "
                + " set WAREHOUSE_SIZE = XSMALL, WAREHOUSE_TYPE = STANDARD, AUTO_SUSPEND = 0, AUTO_RESUME = True'"
            )
            _ = cur.execute(sql).fetchone()

            sql = (
                f"INSERT INTO INTERNAL.WAREHOUSE_ALTER_STATEMENTS SELECT '{id5}', 'alter warehouse {wh_name} "
                + " set WAREHOUSE_SIZE = XXLARGE, WAREHOUSE_TYPE = STANDARD, AUTO_SUSPEND = 15, AUTO_RESUME = True'"
            )
            _ = cur.execute(sql).fetchone()

            # weekday, 16:00 in UTC+1
            row = cur.execute(
                _update_warehouse_schedules_sql(
                    "2023-09-29 07:45:00", "2023-09-29 08:00:00"
                )
            ).fetchone()
            _assert_no_updates(row)

            # weekday, 09:00 in utc+1
            row = cur.execute(
                _update_warehouse_schedules_sql(
                    "2023-09-29 00:45:00", "2023-09-29 01:00:00"
                )
            ).fetchone()

            obj = json.loads(row[0])
            assert "num_candidates" in obj
            assert (
                obj["num_candidates"] == 1
            ), f"Expected to have one candidate schedule: {obj}"
            assert "warehouses_updated" in obj
            assert (
                obj["warehouses_updated"] == 1
            ), f"Expected one warehouse to be updated: {obj}"
            assert "statements" in obj
            assert (
                len(obj["statements"]) == 1
            ), f"Expected an alter warehouse statement: {obj}"
            assert "WAREHOUSE_SIZE = MEDIUM" in obj["statements"][0]
            assert "AUTO_SUSPEND = 15" in obj["statements"][0]

            # weekday, 17:00 in utc+1
            row = cur.execute(
                _update_warehouse_schedules_sql(
                    "2023-09-29 08:45:00", "2023-09-29 09:00:00"
                )
            ).fetchone()

            obj = json.loads(row[0])
            assert "warehouses_updated" in obj
            assert obj["warehouses_updated"] == 1
            assert "num_candidates" in obj
            assert obj["num_candidates"] == 1
            assert "statements" in obj
            assert len(obj["statements"]) == 1
            assert "WAREHOUSE_SIZE = SMALL" in obj["statements"][0]
            assert "AUTO_SUSPEND = 5" in obj["statements"][0]

            # weekend, 09:00 in utc+1
            row = cur.execute(
                _update_warehouse_schedules_sql(
                    "2023-09-30 00:45:00", "2023-09-30 01:00:00"
                )
            ).fetchone()
            _assert_no_updates(row)

            # weekend, 17:00 in utc+1
            row = cur.execute(
                _update_warehouse_schedules_sql(
                    "2023-09-30 08:45:00", "2023-09-30 09:00:00"
                )
            ).fetchone()
            _assert_no_updates(row)

            # weekend, 22:00 in utc+1
            row = cur.execute(
                _update_warehouse_schedules_sql(
                    "2023-09-30 13:45:00", "2023-09-30 14:00:00"
                )
            ).fetchone()
            obj = json.loads(row[0])
            assert "num_candidates" in obj
            assert obj["num_candidates"] == 1
            assert "warehouses_updated" in obj
            assert obj["warehouses_updated"] == 1
            assert "statements" in obj
            assert len(obj["statements"]) == 1
            assert "WAREHOUSE_SIZE = XXLARGE" in obj["statements"][0]
            assert "AUTO_SUSPEND = 15" in obj["statements"][0]
    finally:
        with conn() as cnx, cnx.cursor() as cur:
            _ = cur.execute(f"DROP WAREHOUSE IF EXISTS {wh_name}").fetchone()
