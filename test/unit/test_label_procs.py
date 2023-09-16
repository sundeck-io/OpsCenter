from __future__ import annotations

import pytest
from common_utils import generate_unique_name
from common_utils import run_proc
from common_utils import row_count
from common_utils import run_sql
import time


def test_smoke_create_drop_label(conn, timestamp_string):
    label = generate_unique_name("label", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'rows_produced > 100', FALSE);"

    # create_label returns NULL in case of successful label creation
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

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

    # drop label
    sql = f"call ADMIN.DELETE_LABEL('{label}');"
    assert run_proc(conn, sql) == "done", "Stored procedure did not return NULL value!"


def test_smoke_create_update_drop_dynamic_group_label(conn, timestamp_string):
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
    sql = f"call ADMIN.UPDATE_LABEL(NULL, NULL, '{name}', NULL, 'lower(query_type)', true);"
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
    assert run_proc(conn, sql) == "done", "Stored procedure did not return NULL value!"


def test_smoke_update_label(conn, timestamp_string):
    label = generate_unique_name("label", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'rows_produced > 100', FALSE);"

    # create_label returns NULL in case of successful label creation
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

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

    # update label
    sql = f"call ADMIN.UPDATE_LABEL('{label}', '{label}', NULL, NULL, 'compilation_time > 3000');"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    # make sure it was created with correct properties
    sql = f"""select count(*) from INTERNAL.labels where
                name = '{label}' and
                group_name is null and
                group_rank is null and
                label_created_at is not null and
                condition = 'compilation_time > 3000' and
                enabled
        """
    assert row_count(conn, sql) == 1, "Label was not found!"


# List of test cases with statements and expected error messages
test_cases = [
    (
        "call ADMIN.CREATE_LABEL(NULL, NULL, NULL, 'compilation_time > 5000', FALSE);",
        "Name must not be null.",
    ),
    (
        "call ADMIN.CREATE_LABEL('{label}', NULL, 100, 'compilation_time > 5000', FALSE);",
        "Rank must only be set if Group name is also provided.",
    ),
    (
        "call ADMIN.CREATE_LABEL('{label}', 'group_1', NULL, 'compilation_time > 5000', FALSE);",
        "Rank must provided if you are creating a grouped label.",
    ),
    (
        "call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'compile_time > 5000', FALSE);",
        "Invalid condition SQL. Please check your syntax.",
    ),
    (
        "call ADMIN.CREATE_LABEL('QUERY_TEXT', NULL, NULL, 'compilation_time > 5000', FALSE);",
        "Label name can not be same as column name in view reporting.enriched_query_history. Please use a different label name.",
    ),
    (
        "call ADMIN.UPDATE_LABEL('{label}', NULL, NULL, NULL, 'compile_time > 5000', FALSE);",
        "Name must not be null.",
    ),
    (
        "call ADMIN.UPDATE_LABEL('{label}', '{label}_1', NULL, 100, 'compilation_time > 5000', FALSE);",
        "Rank must only be set if group name is also provided.",
    ),
    (
        "call ADMIN.UPDATE_LABEL('{label}', '{label}_1', 'group_1', NULL, 'compilation_time > 5000', FALSE);",
        "Rank must provided if you are creating a grouped label.",
    ),
    (
        "call ADMIN.UPDATE_LABEL('{label}', '{label}', 'group_1', 100, 'compilation_time > 5000', FALSE);",
        "Label not found. Please refresh your page to see latest list of labels.",
    ),
    (
        "call ADMIN.UPDATE_LABEL('{label}', '{label}', 'group_1', 100, 'compile_time > 5000', FALSE);",
        "Invalid condition SQL. Please check your syntax.",
    ),
    (
        "call ADMIN.CREATE_LABEL('{label}', 'QUERY_TEXT', 10, 'compilation_time > 5000', FALSE);",
        "Group name can not be same as column name in view reporting.enriched_query_history. Please use a different group name.",
    ),
    (
        "call ADMIN.CREATE_LABEL('{label}', 'DYNAMIC_GROUP_LABEL', NULL, 'QUERY_TYPE', TRUE);",
        "Rank or name must not be set for dynamic grouped labels.",
    ),
    (
        "call ADMIN.CREATE_LABEL(NULL, 'DYNAMIC_GROUP_LABEL', 10, 'QUERY_TYPE', TRUE);",
        "Rank or name must not be set for dynamic grouped labels.",
    ),
    (
        "call ADMIN.CREATE_LABEL(NULL, NULL, 10, 'QUERY_TYPE', TRUE);",
        "group name must be set for dynamic grouped labels.",
    ),
    (
        "call ADMIN.UPDATE_LABEL('{label}', '{label}', 'DYNAMIC_GROUP_LABEL', NULL, 'QUERY_TYPE', TRUE);",
        "Rank or name must not be set for dynamic grouped labels.",
    ),
    (
        "call ADMIN.UPDATE_LABEL(NULL, NULL, 'DYNAMIC_GROUP_LABEL', 10, 'QUERY_TYPE', TRUE);",
        "Rank or name must not be set for dynamic grouped labels.",
    ),
    (
        "call ADMIN.DELETE_LABEL(NULL);",
        "Name must not be null.",
    ),
]

# Test that validates that correct error message was returned
@pytest.mark.parametrize("statement, expected_error", test_cases)
def test_error_message(conn, timestamp_string, statement, expected_error):

    label = generate_unique_name("label", timestamp_string)
    sql = statement.format(label=label)
    assert expected_error in str(
        run_proc(conn, sql)
    ), "Stored procedure output does not match expected result!"


# Test that validates that we get correct error on attempt to create label with existing name
def test_create_label_with_existing_name(conn, timestamp_string):

    label = generate_unique_name("label", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'compilation_time > 5000', TRUE);"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    sql = f"call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'compilation_time > 5000');"
    assert (
        run_proc(conn, sql) == "Duplicate label name found. Please use a distinct name."
    ), "Stored procedure output does not match expected result!"


# Test that validates that we get correct errors on attempt to update existing label
def test_update_label_errors(conn, timestamp_string):

    # First label
    label = generate_unique_name("label", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'compilation_time > 5000', FALSE);"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    # Second label
    sql = (
        f"call ADMIN.CREATE_LABEL('{label}_2', NULL, NULL, 'compilation_time > 5000', FALSE);"
    )
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    # Update second label with the name of the first label
    sql = f"call ADMIN.UPDATE_LABEL('{label}_2', '{label}', NULL, NULL, 'rows_produced > 100', FALSE);"
    assert (
        run_proc(conn, sql)
        == "A label with this name already exists. Please choose a distinct name."
    ), "Stored procedure output does not match expected result!"

    # Update label with the column name from reporting.enriched_query_history

    sql = f"call ADMIN.UPDATE_LABEL('{label}_2', 'QUERY_TEXT', NULL, NULL, 'rows_produced > 100', FALSE);"
    assert (
        run_proc(conn, sql)
        == "Label name can not be same as column name in view reporting.enriched_query_history. Please use a different label name."
    ), "Stored procedure output does not match expected result!"

    sql = f"call ADMIN.UPDATE_LABEL('{label}_2', 'QUERY_TEXT', 'group_1', 100, 'rows_produced > 100', FALSE);"
    assert (
        run_proc(conn, sql)
        == "Label name can not be same as column name in view reporting.enriched_query_history. Please use a different label name."
    ), "Stored procedure output does not match expected result!"


# Test that validates that we can create/drop label with empty string for name
# Legal in Snowflake
def test_create_label_with_empty_string_name(conn, timestamp_string):

    sql = "call ADMIN.CREATE_LABEL('', NULL, NULL, 'compilation_time > 5000', FALSE);"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    sql = "call ADMIN.DELETE_LABEL('');"
    assert "done" in str(
        run_proc(conn, sql)
    ), "Stored procedure output does not match expected result!"


# Test that validates that we get correct error on attempt to create grouped label with existing name in same group
def test_create_grouped_label_with_existing_name(conn, timestamp_string):
    label = generate_unique_name("label", timestamp_string)
    sql = (
        f"call ADMIN.CREATE_LABEL('{label}', 'group-1', 10, 'compilation_time > 5000', FALSE);"
    )
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    sql = (
        f"call ADMIN.CREATE_LABEL('{label}', 'group-1', 20, 'compilation_time > 5000', FALSE);"
    )
    assert (
        run_proc(conn, sql)
        == "Duplicate grouped label name found. Please use a distinct name."
    ), "Stored procedure output does not match expected result!"


# Test that validates the behavior when we create ungrouped label, then grouped label
def test_create_ungrouped_then_grouped_and_label(conn, timestamp_string):
    label = generate_unique_name("label", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'compilation_time > 5000', FALSE);"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    # grouped label has its group_name conflicting with the ungrouped label's name.
    sql = f"call ADMIN.CREATE_LABEL('{label}_1', '{label}', 20, 'compilation_time > 5000', FALSE);"
    assert (
        run_proc(conn, sql)
        == "Duplicate grouped label name found. Please use a distinct name."
    ), "Stored procedure output does not match expected result!"


# Test that validates the behavior when we create grouped label, then ungrouped label
def test_create_grouped_then_ungrouped_label(conn, timestamp_string):
    label = generate_unique_name("label", timestamp_string)
    ## create a grouped label, with group_name = {label}
    sql = f"call ADMIN.CREATE_LABEL('{label}_1', '{label}', 10, 'compilation_time > 5000', FALSE);"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    # create an ungrouped label using label_name = {label}.
    sql = f"call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'compilation_time > 5000', FALSE);"
    assert (
        run_proc(conn, sql) == "Duplicate label name found. Please use a distinct name."
    ), "Stored procedure output does not match expected result!"


def test_validate_predefined_label(conn, timestamp_string):
    sql = "CALL INTERNAL.VALIDATE_PREDEFINED_LABELS();"
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

    # step 7: sleep 5 seconds
    time.sleep(5)

    # step 7: call internal.migrate_predefined_labels()
    sql = "call INTERNAL.MIGRATE_PREDEFINED_LABELS(5)"
    output = str(run_sql(conn, sql))
    assert "True" in output, "SQL output" + output + " does not match expected result!"

    # step 8: insert a new predefined label to PREDEFIEND_LABELS
    sql = "INSERT INTO INTERNAL.PREDEFINED_LABELS (name, condition, LABEL_CREATED_AT, LABEL_MODIFIED_AT) values ('NEW PREDEFINED LABEL', 'rows_produced > 50 ', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
    run_sql(conn, sql)

    # step 9: call internal.migrate_predefined_labels()
    sql = "call INTERNAL.MIGRATE_PREDEFINED_LABELS(5)"
    output = str(run_sql(conn, sql))
    assert "True" in output, "SQL output" + output + " does not match expected result!"

    # step 10: verify labels table has the new added predefinend label "NEW PREDEFINED LABEL"
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
