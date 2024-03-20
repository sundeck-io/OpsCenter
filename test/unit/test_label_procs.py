from __future__ import annotations

import pytest
from common_utils import generate_unique_name
from common_utils import run_proc
from common_utils import row_count
from common_utils import run_sql
import time


@pytest.mark.smoke
def test_smoke_ungrouped_label(conn, timestamp_string):
    label = generate_unique_name("label", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'rows_produced > 100');"

    # create_label returns NULL in case of successful label creation
    assert run_proc(conn, sql) is None, "ADMIN.CREATE_LABEL did not return NULL value!"

    # make sure it was created with correct properties
    sql = f"""select count(*) from INTERNAL.labels where
                 name = '{label}' and
                 group_name is null and
                 group_rank is null and
                 label_created_at is not null and
                 condition = 'rows_produced > 100' and
                 enabled
        """
    assert row_count(conn, sql) == 1, "Label was not found!"

    # update condition expression
    new_name = f"{label}_new"
    sql = f"call ADMIN.UPDATE_LABEL('{label}', '{new_name}', NULL, NULL, 'rows_produced > 1000');"
    assert run_proc(conn, sql) is None, "ADMIN.UPDATE_LABEL did not return NULL value!"

    # make sure the condition is updated
    sql = f"""select count(*) from INTERNAL.labels where
                        name = '{new_name}' and
                        group_name is null and
                        group_rank is null and
                        label_created_at is not null and
                        condition = 'rows_produced > 1000' and
                        enabled  and
                        not is_dynamic
               """
    assert row_count(conn, sql) == 1, "Grouped label after update was not found!"

    # drop label
    sql = f"call ADMIN.DELETE_LABEL('{label}');"
    assert run_proc(conn, sql) is None, "ADMIN.DELETE_LABEL did not return NULL value!"


@pytest.mark.smoke
def test_smoke_grouped_label(conn, timestamp_string):
    name = generate_unique_name("label", timestamp_string)
    group_name = generate_unique_name("group", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL('{name}', '{group_name}', 50, 'query_type = \\'SELECT\\'');"

    # create_label returns NULL in case of successful label creation
    assert run_proc(conn, sql) is None, "ADMIN.CREATE_LABEL did not return NULL value!"

    # make sure it was created with correct properties
    sql = f"""select count(*) from INTERNAL.labels where
                    name = '{name}' and
                    group_name = '{group_name}' and
                    group_rank = 50 and
                    label_created_at is not null and
                    condition = 'query_type = \\\'SELECT\\\'' and
                    enabled and
                    not is_dynamic
           """
    assert row_count(conn, sql) == 1, "Grouped label was not found!"

    # update condition expression
    new_name = f"{name}_new"
    sql = f"call ADMIN.UPDATE_LABEL('{name}', '{new_name}', '{group_name}', 100, 'query_type ilike \\'select\\'');"
    assert run_proc(conn, sql) is None, "ADMIN.UPDATE_LABEL did not return NULL value!"

    # make sure the condition is updated
    sql = f"""select count(*) from INTERNAL.labels where
                        name = '{new_name}' and
                        group_name = '{group_name}' and
                        group_rank = 100 and
                        label_created_at is not null and
                        condition = 'query_type ilike \\\'select\\\'' and
                        enabled  and
                        not is_dynamic
               """
    assert row_count(conn, sql) == 1, "Grouped label after update was not found!"

    # drop label
    sql = f"call ADMIN.DELETE_DYNAMIC_LABEL('{name}');"
    assert run_proc(conn, sql) is None, "ADMIN.DELETE_LABEL did not return NULL value!"


@pytest.mark.smoke
def test_smoke_dynamic_grouped_label(conn, timestamp_string):
    name = generate_unique_name("group", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL(NULL, '{name}', NULL, 'query_type', true);"

    # create_label returns NULL in case of successful label creation
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    # make sure it was created with correct properties
    sql = f"""select count(*) from INTERNAL.labels where
                    name is null and
                    group_name = '{name}' and
                    group_rank is null and
                    label_created_at is not null and
                    condition = 'query_type' and
                    enabled and
                    is_dynamic
           """
    assert row_count(conn, sql) == 1, "Dynamic label was not found!"

    # update condition expression
    sql = f"call ADMIN.UPDATE_LABEL('{name}', NULL, '{name}', NULL, 'lower(query_type)', true);"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    # make sure the condition is updated
    sql = f"""select count(*) from INTERNAL.labels where
                        name is null and
                        group_name = '{name}' and
                        group_rank is null and
                        label_created_at is not null and
                        condition = 'lower(query_type)' and
                        enabled  and
                        is_dynamic
               """
    assert row_count(conn, sql) == 1, "Dynamic label after update was not found!"

    # drop label
    sql = f"call ADMIN.DELETE_DYNAMIC_LABEL('{name}');"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"


# List of test cases with statements and expected error messages
test_cases = [
    (
        "call ADMIN.CREATE_LABEL(NULL, NULL, NULL, 'compilation_time > 5000');",
        "Name must not be null",
    ),
    (
        "call ADMIN.CREATE_LABEL('{label}', NULL, 100, 'compilation_time > 5000');",
        "Group rank may only be provided for grouped labels",
    ),
    (
        "call ADMIN.CREATE_LABEL('{label}', 'group_1', NULL, 'compilation_time > 5000');",
        "Grouped labels must have a rank",
    ),
    (
        "call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'compile_time > 5000');",
        "Invalid label condition",
    ),
    (
        "call ADMIN.CREATE_LABEL('QUERY_TEXT', NULL, NULL, 'compilation_time > 5000');",
        "Label name cannot be the same as a column in REPORTING.ENRICHED_QUERY_HISTORY",
    ),
    (
        "call ADMIN.UPDATE_LABEL('{label}', NULL, NULL, NULL, 'compile_time > 5000');",
        "Name must not be null",
    ),
    (
        "call ADMIN.UPDATE_LABEL('{label}', '{label}_1', NULL, 100, 'compilation_time > 5000');",
        "Group rank may only be provided for grouped labels",
    ),
    (
        "call ADMIN.UPDATE_LABEL('{label}', '{label}_1', 'group_1', NULL, 'compilation_time > 5000');",
        "Grouped labels must have a rank",
    ),
    (
        "call ADMIN.UPDATE_LABEL('{label}', '{label}', 'group_1', 100, 'compilation_time > 5000');",
        "does not exist",
    ),
    (
        "call ADMIN.UPDATE_LABEL('{label}', '{label}', 'group_1', 100, 'compile_time > 5000');",
        "Invalid label condition",
    ),
    (
        "call ADMIN.CREATE_LABEL('{label}', 'QUERY_TEXT', 10, 'compilation_time > 5000');",
        "Label group name cannot be the same as a column in REPORTING.ENRICHED_QUERY_HISTORY",
    ),
    (
        "call ADMIN.CREATE_LABEL('{label}', 'DYNAMIC_GROUP_LABEL', NULL, 'QUERY_TYPE', TRUE);",
        "Dynamic labels cannot have a name",
    ),
    (
        "call ADMIN.CREATE_LABEL(NULL, 'DYNAMIC_GROUP_LABEL', 10, 'QUERY_TYPE', TRUE);",
        "Dynamic labels cannot have a rank",
    ),
    (
        "call ADMIN.CREATE_LABEL(NULL, NULL, NULL, 'QUERY_TYPE', TRUE);",
        "Dynamic labels must have a group name",
    ),
    (
        "call ADMIN.UPDATE_LABEL('{label}', '{label}', 'DYNAMIC_GROUP_LABEL', NULL, 'QUERY_TYPE', TRUE);",
        "Dynamic labels cannot have a name",
    ),
    (
        "call ADMIN.UPDATE_LABEL('DYNAMIC_GROUP_LABEL', NULL, 'DYNAMIC_GROUP_LABEL', 10, 'QUERY_TYPE', TRUE);",
        "Dynamic labels cannot have a rank",
    ),
    (
        "call ADMIN.DELETE_DYNAMIC_LABEL(NULL);",
        "Name must not be null",
    ),
]

# Test that validates that correct error message was returned
@pytest.mark.parametrize("statement, expected_error", test_cases)
def test_error_message(conn, timestamp_string, statement, expected_error):

    label = generate_unique_name("label", timestamp_string)
    sql = statement.format(label=label)
    assert expected_error in str(run_proc(conn, sql))


# Test that validates that we get correct error on attempt to create label with existing name
def test_create_label_with_existing_name(conn, timestamp_string):

    label = generate_unique_name("label", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'compilation_time > 5000');"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    sql = f"call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'compilation_time > 5000');"
    assert "already exists" in run_proc(conn, sql)


# Test that validates that we get correct errors on attempt to update existing label
def test_update_label_errors(conn, timestamp_string):

    # First label
    label = generate_unique_name("label", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'compilation_time > 5000');"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    # Second label
    sql = (
        f"call ADMIN.CREATE_LABEL('{label}_2', NULL, NULL, 'compilation_time > 5000');"
    )
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    # Update second label with the name of the first label
    sql = f"call ADMIN.UPDATE_LABEL('{label}_2', '{label}', NULL, NULL, 'rows_produced > 100');"
    assert "already exists" in run_proc(conn, sql)

    # Ungrouped label names cannot conflict with columns in ENRICHED_QUERY_HISTORY
    sql = f"call ADMIN.UPDATE_LABEL('{label}_2', 'QUERY_TEXT', NULL, NULL, 'rows_produced > 100');"
    assert (
        "cannot be the same as a column in REPORTING.ENRICHED_QUERY_HISTORY"
        in run_proc(conn, sql)
    )

    # Grouped label names cannot conflict with columns in ENRICHED_QUERY_HISTORY, but the name on
    # a grouped label _may_ (because the grouped label's name is the column value in the view)
    sql = f"call ADMIN.UPDATE_LABEL('{label}_2', '{label}_2', 'QUERY_TEXT', 100, 'rows_produced > 100');"
    assert (
        "cannot be the same as a column in REPORTING.ENRICHED_QUERY_HISTORY"
        in run_proc(conn, sql)
    )


# Test that validates that we can create/drop label with empty string for name
# Legal in Snowflake
def test_create_label_with_empty_string_name(conn, timestamp_string):

    sql = "call ADMIN.CREATE_LABEL('', NULL, NULL, 'compilation_time > 5000');"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    sql = "call ADMIN.DELETE_LABEL('');"
    assert run_proc(conn, sql) is None


# Test that validates that we get correct error on attempt to create grouped label with existing name in same group
def test_create_grouped_label_with_existing_name(conn, timestamp_string):
    label = generate_unique_name("label", timestamp_string)
    sql = (
        f"call ADMIN.CREATE_LABEL('{label}', 'group-1', 10, 'compilation_time > 5000');"
    )
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    sql = (
        f"call ADMIN.CREATE_LABEL('{label}', 'group-1', 20, 'compilation_time > 5000');"
    )

    assert "A label with this name already exists" in run_proc(conn, sql)


# Test that validates the behavior when we create ungrouped label, then grouped label
def test_create_ungrouped_then_grouped_and_label(conn, timestamp_string):
    label = generate_unique_name("label", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'compilation_time > 5000');"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    # grouped label has its group_name conflicting with the ungrouped label's name.
    sql = f"call ADMIN.CREATE_LABEL('{label}_1', '{label}', 20, 'compilation_time > 5000');"
    assert "A label with this name already exists" in run_proc(conn, sql)


# Test that validates the behavior when we create grouped label, then ungrouped label
def test_create_grouped_then_ungrouped_label(conn, timestamp_string):
    label = generate_unique_name("label", timestamp_string)
    ## create a grouped label, with group_name = {label}
    sql = f"call ADMIN.CREATE_LABEL('{label}_1', '{label}', 10, 'compilation_time > 5000');"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    # create an ungrouped label using label_name = {label}.
    sql = f"call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'compilation_time > 5000');"
    assert "already exists" in run_proc(conn, sql)


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


def test_merge_predefined_labels(conn, timestamp_string):
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
    sql = "call INTERNAL.MERGE_PREDEFINED_LABELS()"
    assert run_proc(conn, sql) is None, "Expected no return value from procedure"

    # verify the same number of rows in labels table
    sql = "select count(*) from internal.LABELS"
    num_labels = row_count(conn, sql)
    assert (
        num_labels == num_predefined_labels
    ), "Number of user labels did not match predefined labels"

    sql = "select count(*) from internal.LABELS where ENABLED is null"
    assert (
        row_count(conn, sql) == 0
    ), "Labels created by merge_predefined_labels should all be enabled"
