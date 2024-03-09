from __future__ import annotations

import pytest
from common_utils import (
    generate_unique_name,
    run_proc,
    row_count,
    run_sql,
)
import time


def test_validate_predefined_label(conn, timestamp_string):
    pytest.skip("needs snowflake-snowpark-python>=1.13.0")
    sql = "CALL INTERNAL_PYTHON.VALIDATE_PREDEFINED_LABELS();"
    assert run_proc(conn, sql) is None


def test_initialize_labels(conn, timestamp_string):
    # step 1: clean up the labels table and predefined_labels table
    sql = "truncate table internal.labels"
    assert "successfully" in str(
        run_sql(conn, sql)
    ), "SQL output does not match expected result!"

    sql = "truncate table internal.predefined_labels"
    assert "successfully" in str(
        run_sql(conn, sql)
    ), "SQL output does not match expected result!"

    # step 2: clean up the flag in internal.config
    sql = "delete from internal.config where KEY = 'LABELS_INITED'"
    run_sql(conn, sql)

    # step 3: populate predefined_labels table
    sql = "CALL INTERNAL.POPULATE_PREDEFINED_LABELS();"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    sql = "select count(*) from internal.PREDEFINED_LABELS"
    output = row_count(conn, sql)
    assert output > 0, "SQL output " + str(output) + " does not match expected result!"

    # step 4: call internal.initialize_labels()
    sql = "call INTERNAL.INITIALIZE_LABELS()"
    output = str(run_sql(conn, sql))
    assert "True" in output, "SQL output" + output + " does not match expected result!"

    # step 5: verify rows in labels table
    sql = "select count(*) from internal.LABELS"
    output = row_count(conn, sql)
    assert output > 0, "SQL output " + str(output) + " does not match expected result!"

    # step 6: verify flag in internal.config
    sql = "call internal.get_config('LABELS_INITED')"
    output = str(run_sql(conn, sql))
    assert "True" in output, "SQL output" + output + " does not match expected result!"

    # step 7: call internal.initialize_labels() again
    sql = "call INTERNAL.INITIALIZE_LABELS()"
    output = str(run_sql(conn, sql))
    assert "False" in output, "SQL output" + output + " does not match expected result!"

    # step 8: verify rows in labels table
    sql = "select count(*) from internal.LABELS"
    output = row_count(conn, sql)
    assert output > 0, "SQL output " + str(output) + " does not match expected result!"

    # step 9: clean up the labels table and predefined_labels table
    sql = "truncate table internal.labels"
    assert "successfully" in str(
        run_sql(conn, sql)
    ), "SQL output does not match expected result!"

    sql = "truncate table internal.predefined_labels"
    assert "successfully" in str(
        run_sql(conn, sql)
    ), "SQL output does not match expected result!"


def test_migrate_predefined_labels(conn, timestamp_string):
    # step 1: clean up the labels table and predefined_labels table
    sql = "truncate table internal.labels"
    assert "successfully" in str(
        run_sql(conn, sql)
    ), "SQL output does not match expected result!"

    sql = "truncate table internal.predefined_labels"
    assert "successfully" in str(
        run_sql(conn, sql)
    ), "SQL output does not match expected result!"

    sql = "delete from internal.config where KEY = 'LABELS_INITED'"
    run_sql(conn, sql)

    # step 3: populate predefined_labels table
    sql = "CALL INTERNAL.POPULATE_PREDEFINED_LABELS();"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    sql = "select count(*) from internal.PREDEFINED_LABELS"
    num_predefined_labels = row_count(conn, sql)
    assert (
        num_predefined_labels > 0
    ), f"SQL output {num_predefined_labels} does not match expected result!"

    # step 4: call internal.initialize_labels()
    sql = "call INTERNAL.INITIALIZE_LABELS()"
    output = str(run_sql(conn, sql))
    assert "True" in output, "SQL output" + output + " does not match expected result!"

    # step 5: verify rows in labels table
    sql = "select count(*) from internal.LABELS"
    num_labels = row_count(conn, sql)
    assert (
        num_labels == num_predefined_labels
    ), "Number of user labels did not match predefined labels"

    # step 6: verify flag in internal.config
    sql = "call internal.get_config('LABELS_INITED')"
    output = str(run_sql(conn, sql))
    assert "True" in output, "SQL output" + output + " does not match expected result!"

    # step 7: sleep 5 seconds
    time.sleep(5)

    # step 7: call internal.migrate_predefined_labels()
    sql = "call INTERNAL.MIGRATE_PREDEFINED_LABELS(5)"
    output = str(run_sql(conn, sql))
    assert "True" in output, "SQL output" + output + " does not match expected result!"

    # Verify that a migration does not create new labels
    sql = "select count(*) from internal.LABELS"
    num_labels_after_migrate = row_count(conn, sql)
    assert (
        num_labels == num_labels_after_migrate
    ), "Number of user labels did not match predefined labels after what-should-be a no-op migration"

    # step 8: insert a new predefined label to PREDEFINED_LABELS
    sql = "INSERT INTO INTERNAL.PREDEFINED_LABELS (name, condition, LABEL_CREATED_AT, LABEL_MODIFIED_AT) values ('NEW PREDEFINED LABEL', 'rows_produced > 50 ', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
    run_sql(conn, sql)

    # step 9: call internal.migrate_predefined_labels()
    sql = "call INTERNAL.MIGRATE_PREDEFINED_LABELS(5)"
    output = str(run_sql(conn, sql))
    assert "True" in output, "SQL output" + output + " does not match expected result!"

    # step 10: verify labels table has the new added predefined label "NEW PREDEFINED LABEL"
    sql = "select count(*) from internal.LABELS where name = 'NEW PREDEFINED LABEL'"
    rowcount2 = row_count(conn, sql)
    assert rowcount2 == 1, (
        "SQL output " + str(rowcount2) + " does not match expected result!"
    )

    # step 11: update the condition of 'NEW PREDEFINED LABEL'
    sql = "UPDATE INTERNAL.PREDEFINED_LABELS SET CONDITION = 'rows_produced > 100 ' where NAME = 'NEW PREDEFINED LABEL'"
    run_sql(conn, sql)

    # step 12: call internal.migrate_predefined_labels()
    time.sleep(5)

    sql = "call INTERNAL.MIGRATE_PREDEFINED_LABELS(5)"
    output = str(run_sql(conn, sql))
    assert "True" in output, "SQL output" + output + " does not match expected result!"

    # step 13: insert a row into user's labels
    sql = (
        "INSERT INTO INTERNAL.LABELS (name, condition) values ('test', 'testcondition')"
    )
    run_sql(conn, sql)

    ## step 14: MIGRATE_PREDEFINED_LABELS should return False, because user adds one label
    sql = "call INTERNAL.MIGRATE_PREDEFINED_LABELS(5)"
    output = str(run_sql(conn, sql))
    assert "False" in output, "SQL output" + output + " does not match expected result!"

    ## step 15: clean up data
    sql = "truncate table internal.labels"
    assert "successfully" in str(
        run_sql(conn, sql)
    ), "SQL output does not match expected result!"

    sql = "truncate table internal.predefined_labels"
    assert "successfully" in str(
        run_sql(conn, sql)
    ), "SQL output does not match expected result!"

    sql = "delete from internal.config where KEY = 'LABELS_INITED'"
    run_sql(conn, sql)


def test_fixes_duplicate_labels(conn, timestamp_string):
    # clean up the labels table and predefined_labels table
    sql = "truncate table internal.labels"
    assert "successfully" in str(
        run_sql(conn, sql)
    ), "SQL output does not match expected result!"

    sql = "truncate table internal.predefined_labels"
    assert "successfully" in str(
        run_sql(conn, sql)
    ), "SQL output does not match expected result!"

    sql = "delete from internal.config where KEY = 'LABELS_INITED'"
    run_sql(conn, sql)

    # Populate predefined_labels table, make sure we have some predefined_labels
    sql = "CALL INTERNAL.POPULATE_PREDEFINED_LABELS();"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    sql = "select count(*) from internal.PREDEFINED_LABELS"
    num_predefined_labels = row_count(conn, sql)
    assert (
        num_predefined_labels > 0
    ), f"SQL output {num_predefined_labels} does not match expected result!"

    # Copy the predefined labels into the labels table
    sql = "call INTERNAL.INITIALIZE_LABELS()"
    output = str(run_sql(conn, sql))
    assert "True" in output, "SQL output" + output + " does not match expected result!"

    # verify the number of rows in labels table
    sql = "select count(*) from internal.LABELS"
    num_labels = row_count(conn, sql)
    assert (
        num_labels == num_predefined_labels
    ), "Number of user labels did not match predefined labels"

    # Create some grouped labels
    group_name = generate_unique_name("group", timestamp_string)
    assert (
        run_proc(
            conn,
            f"call ADMIN.CREATE_LABEL('l1', '{group_name}', 50, 'query_type = \\'SELECT\\'');",
        )
        is None
    ), "did not create grouped label"
    assert (
        run_proc(
            conn,
            f"call ADMIN.CREATE_LABEL('l2', '{group_name}', 60, 'query_type = \\'INSERT\\'');",
        )
        is None
    ), "did not create grouped label"

    # Insert duplicate grouped labels
    num_inserts = row_count(
        conn,
        "INSERT INTO INTERNAL.LABELS SELECT *,uuid_string() as label_id FROM (select * exclude (label_id) from INTERNAL.LABELS) WHERE GROUP_NAME IS NOT NULL",
    )
    assert num_inserts > 0, "Should have created some duplicates"

    # Insert duplicate ungrouped labels
    num_inserts = row_count(
        conn,
        "INSERT INTO INTERNAL.LABELS SELECT *,uuid_string() as label_id FROM (select * exclude (label_id) from INTERNAL.LABELS) WHERE GROUP_NAME IS NULL",
    )
    assert num_inserts > 0, "Should have created some duplicates"

    # Call migrate_predefined_labels which should remove any duplicate from internal.labels
    sql = "call INTERNAL.REMOVE_DUPLICATE_LABELS()"
    output = run_proc(conn, sql)
    assert output is None, "failed to remove duplicate labels"

    # Verify migration removed duplicates (num predefined labels + two grouped labels made)
    num_labels_after_migrate = row_count(conn, "select count(*) from internal.LABELS")
    assert (
        num_predefined_labels + 2 == num_labels_after_migrate
    ), "Duplicate labels should have been removed"

    grp_count = row_count(
        conn, f"select count(*) from internal.labels where group_name = '{group_name}'"
    )
    assert (
        grp_count == 2
    ), f"Should have two grouped labels with group name '{group_name}'"

    # Cleanup for the next test
    sql = "truncate table internal.labels"
    assert "successfully" in str(
        run_sql(conn, sql)
    ), "SQL output does not match expected result!"

    sql = "truncate table internal.predefined_labels"
    assert "successfully" in str(
        run_sql(conn, sql)
    ), "SQL output does not match expected result!"

    sql = "delete from internal.config where KEY = 'LABELS_INITED'"
    run_sql(conn, sql)
