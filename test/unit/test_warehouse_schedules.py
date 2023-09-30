from __future__ import annotations

import json
import uuid
from common_utils import generate_unique_name


def test_basic_warehouse_schedule(conn, timestamp_string):
    wh_name = generate_unique_name("wh", timestamp_string).replace("-", "_")
    try:
        with conn() as cnx, cnx.cursor() as cur:
            # Ease local dev if we have run materialization
            _ = cur.execute(
                "call internal_python.create_table('WAREHOUSE_SCHEDULES')"
            ).fetchone()
            _ = cur.execute(
                "call internal_python.create_table('WAREHOUSE_ALTER_STATEMENTS')"
            ).fetchone()
            cur.execute("truncate table internal.WH_SCHEDULES").fetchone()

            _ = cur.execute(
                f"CREATE OR REPLACE WAREHOUSE {wh_name} WITH WAREHOUSE_SIZE = XSMALL"
            ).fetchone()

            id1 = uuid.uuid4().hex
            id2 = uuid.uuid4().hex

            # TODO Write and use crud-backed admin procedures. These procedures would generate the alter statement for us.
            sql = f"""INSERT INTO INTERNAL.WH_SCHEDULES select '{id1}', '{wh_name}', '00:00:00', '12:00:00',
                'X-Small', 1, TRUE, 0, 0, 'Standard', NULL, TRUE, null, TRUE"""
            _ = cur.execute(sql).fetchone()

            sql = f"""INSERT INTO INTERNAL.WH_SCHEDULES select '{id2}', '{wh_name}', '12:00:00', '23:59:00',
                'Small', 5, TRUE, 0, 0, 'Standard', NULL, TRUE, null, TRUE"""
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

            # Should do nothing be we have no new schedule
            row = cur.execute(
                """call INTERNAL.UPDATE_WAREHOUSE_SCHEDULES(
                CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', to_timestamp_ltz('2023-09-29 10:00:00')),
                CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', to_timestamp_ltz('2023-09-29 10:15:00')));"""
            ).fetchone()

            obj = json.loads(row[0])
            assert "warehouses_updated" in obj
            assert obj["warehouses_updated"] == 0
            assert "num_candidates" in obj
            assert obj["num_candidates"] == 0

            row = cur.execute(
                """call INTERNAL.UPDATE_WAREHOUSE_SCHEDULES(
                CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', to_timestamp_ltz('2023-09-29 11:45:00')),
                CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', to_timestamp_ltz('2023-09-29 12:00:00')));"""
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
