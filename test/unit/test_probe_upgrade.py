from __future__ import annotations

from common_utils import run_proc
from common_utils import row_count
from common_utils import run_sql
import time


def test_initialize_then_migrate_probes(conn, timestamp_string):
    # step 1: clean up the probes table and predefined_probes table
    sql = "truncate table internal.probes"
    assert "successfully" in str(
        run_sql(conn, sql)
    ), "SQL output does not match expected result!"

    sql = "truncate table internal.predefined_probes"
    assert "successfully" in str(
        run_sql(conn, sql)
    ), "SQL output does not match expected result!"

    # step 2: clean up the flag in internal.config
    sql = "delete from internal.config where KEY = 'PROBES_INITED'"
    run_sql(conn, sql)

    # step 3: populate predefined_probes table
    sql = "CALL INTERNAL.POPULATE_PREDEFINED_PROBES();"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    sql = "select count(*) from internal.PREDEFINED_PROBES"
    output = row_count(conn, sql)
    assert output > 0, "SQL output " + str(output) + " does not match expected result!"

    # step 4: call internal.initialize_probes()
    sql = "call INTERNAL.INITIALIZE_PROBES()"
    output = str(run_sql(conn, sql))
    assert "True" in output, "SQL output" + output + " does not match expected result!"

    # step 5: verify rows in probes table
    sql = "select count(*) from internal.PROBES"
    output = row_count(conn, sql)
    assert output > 0, "SQL output " + str(output) + " does not match expected result!"

    # step 6: verify flag in internal.config
    sql = "call internal.get_config('PROBES_INITED')"
    output = str(run_sql(conn, sql))
    assert "True" in output, "SQL output" + output + " does not match expected result!"

    # step 7: call internal.initialize_probes() again
    sql = "call INTERNAL.INITIALIZE_PROBES()"
    output = str(run_sql(conn, sql))
    assert "False" in output, "SQL output" + output + " does not match expected result!"

    # step 8: verify rows in probes table
    sql = "select count(*) from internal.PROBES"
    output = row_count(conn, sql)
    assert output > 0, "SQL output " + str(output) + " does not match expected result!"

    # step 9: sleep 5 seconds
    time.sleep(5)

    # step 10: call internal.migrate_predefined_probes()
    sql = "call INTERNAL.MIGRATE_PREDEFINED_PROBES(5)"
    output = str(run_sql(conn, sql))
    assert "True" in output, "SQL output" + output + " does not match expected result!"

    # step 11: insert a new predefined probe to PREDEFIEND_PROBES
    sql = "INSERT INTO INTERNAL.PREDEFINED_PROBES (name, condition, PROBE_CREATED_AT, PROBE_MODIFIED_AT) values ('NEW PREDEFINED PROBE', 'bytes_scanned > 10000', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
    run_sql(conn, sql)

    # step 12: call internal.migrate_predefined_probes(). Migration should return true, since we have a new predefined probe.
    sql = "call INTERNAL.MIGRATE_PREDEFINED_PROBES(5)"
    output = str(run_sql(conn, sql))
    assert "True" in output, "SQL output" + output + " does not match expected result!"

    # step 13: verify probes table has the new added predefinend probe "NEW PREDEFINED PROBE"
    sql = "select count(*) from internal.PROBES where name = 'NEW PREDEFINED PROBE'"
    rowcount = row_count(conn, sql)
    assert rowcount == 1, (
        "SQL output " + str(rowcount) + " does not match expected result!"
    )

    # step 14: update the condition of 'NEW PREDEFINED PROBE'
    sql = "UPDATE INTERNAL.PREDEFINED_PROBES SET CONDITION = 'bytes_scanned > 20000 ' where NAME = 'NEW PREDEFINED PROBE'"
    run_sql(conn, sql)

    # step 15: call internal.migrate_predefined_probes(). Migration should return true, since we modify condition of one old predefined probes.
    time.sleep(5)

    sql = "call INTERNAL.MIGRATE_PREDEFINED_probes(5)"
    output = str(run_sql(conn, sql))
    assert "True" in output, "SQL output" + output + " does not match expected result!"

    # step 16: insert a row into user's PROBES
    sql = "INSERT INTO INTERNAL.PROBES (name, condition, PROBE_CREATED_AT, PROBE_MODIFIED_AT) values ('test', 'bytes_scanned > 100', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
    run_sql(conn, sql)

    ## step 17: MIGRATE_PREDEFINED_PROBES should return False, because user adds one PROBE
    sql = "call INTERNAL.MIGRATE_PREDEFINED_PROBES(5)"
    output = str(run_sql(conn, sql))
    assert "False" in output, "SQL output" + output + " does not match expected result!"

    # step 18: clean up the probes table and predefined_probes table
    sql = "truncate table internal.probes"
    assert "successfully" in str(
        run_sql(conn, sql)
    ), "SQL output does not match expected result!"

    sql = "truncate table internal.predefined_probes"
    assert "successfully" in str(
        run_sql(conn, sql)
    ), "SQL output does not match expected result!"


def test_probe_migration(conn):
    with conn() as cnx, cnx.cursor() as cur:
        # If a user never opened OPSCENTER, ADMIN.FINALIZE_SETUP() never could run and the predefined_probes tables
        # be left in an old state because it did not get proper migration.

        # mess up the table
        cur.execute(
            "CREATE OR REPLACE TABLE INTERNAL.PREDEFINED_PROBES (name string, condition string, email_writer boolean, email_other string, cancel boolean, enabled boolean)"
        )

        # call migration
        cur.execute("CALL INTERNAL.MIGRATE_PREDEFINED_PROBES_TABLE()")

        # verify table got fixed
        rows = cur.execute(
            "select column_name, data_type from information_schema.columns where table_name = 'PREDEFINED_PROBES' and table_schema = 'INTERNAL'"
        ).fetchall()
        columns_to_types = {row[0]: row[1] for row in rows}

        assert "EMAIL_WRITER" not in columns_to_types
        assert "EMAIL_OTHER" not in columns_to_types

        assert "NOTIFY_WRITER" in columns_to_types
        assert columns_to_types["NOTIFY_WRITER"] == "BOOLEAN"
        assert "NOTIFY_WRITER_METHOD" in columns_to_types
        assert columns_to_types["NOTIFY_WRITER_METHOD"] == "TEXT"

        assert "NOTIFY_OTHER" in columns_to_types
        assert columns_to_types["NOTIFY_OTHER"] == "TEXT"
        assert "NOTIFY_OTHER_METHOD" in columns_to_types
        assert columns_to_types["NOTIFY_OTHER_METHOD"] == "TEXT"
