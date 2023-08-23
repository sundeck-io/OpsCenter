from __future__ import annotations

from common_utils import run_proc
from common_utils import row_count
from common_utils import run_sql


def test_initialize_probes(conn, timestamp_string):
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

    # step 9: clean up the probes table and predefined_probes table
    sql = "truncate table internal.probes"
    assert "successfully" in str(
        run_sql(conn, sql)
    ), "SQL output does not match expected result!"

    sql = "truncate table internal.predefined_probes"
    assert "successfully" in str(
        run_sql(conn, sql)
    ), "SQL output does not match expected result!"
