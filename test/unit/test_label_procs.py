from __future__ import annotations

import pytest
from common_utils import generate_unique_name
from common_utils import run_proc
from common_utils import validate_row_count


def test_smoke_create_drop_label(conn_cnx, timestamp_string):
    label = generate_unique_name("label", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'rows_produced > 100');"

    # create_label returns NULL in case of successful label creation
    assert (
        run_proc(conn_cnx, sql)[0] is None
    ), "Stored procedure did not return NULL value!"

    # make sure it was created with correct properties
    sql = f"""select count(*) from INTERNAL.labels where
                 name = '{label}' and
                 group_name is null and
                 group_rank is null and
                 label_created_at is not null and
                 condition = 'rows_produced > 100' and
                 enabled is null
        """
    assert validate_row_count(conn_cnx, sql)[0] == 1, "Label was not found!"

    # drop label
    sql = f"call ADMIN.DELETE_LABEL('{label}');"
    assert (
        run_proc(conn_cnx, sql)[0] == "done"
    ), "Stored procedure did not return NULL value!"


def test_smoke_update_label(conn_cnx, timestamp_string):
    label = generate_unique_name("label", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'rows_produced > 100');"

    # create_label returns NULL in case of successful label creation
    assert (
        run_proc(conn_cnx, sql)[0] is None
    ), "Stored procedure did not return NULL value!"

    # make sure it was created with correct properties
    sql = f"""select count(*) from INTERNAL.labels where
                name = '{label}' and
                group_name is null and
                group_rank is null and
                label_created_at is not null and
                condition = 'rows_produced > 100' and
                enabled is null
        """
    assert validate_row_count(conn_cnx, sql)[0] == 1, "Label was not found!"

    # update label
    sql = f"call ADMIN.UPDATE_LABEL('{label}', '{label}', NULL, NULL, 'compilation_time > 3000');"
    assert (
        run_proc(conn_cnx, sql)[0] is None
    ), "Stored procedure did not return NULL value!"

    # make sure it was created with correct properties
    sql = f"""select count(*) from INTERNAL.labels where
                name = '{label}' and
                group_name is null and
                group_rank is null and
                label_created_at is not null and
                condition = 'compilation_time > 3000' and
                enabled is null
        """
    assert validate_row_count(conn_cnx, sql)[0] == 1, "Label was not found!"


# List of test cases with statements and expected error messages
test_cases = [
    (
        "call ADMIN.CREATE_LABEL(NULL, NULL, NULL, 'compilation_time > 5000');",
        "Name must not be null.",
    ),
    (
        "call ADMIN.CREATE_LABEL('{label}', NULL, 100, 'compilation_time > 5000');",
        "Rank must only be set if Group name is also provided.",
    ),
    (
        "call ADMIN.CREATE_LABEL('{label}', 'group_1', NULL, 'compilation_time > 5000');",
        "Rank must provided if you are creating a grouped label.",
    ),
    (
        "call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'compile_time > 5000');",
        "Invalid condition SQL. Please check your syntax.",
    ),
    (
        "call ADMIN.CREATE_LABEL('QUERY_TEXT', 'group_1', 100, 'compilation_time > 5000');",
        "Label name can not be same as column name in view reporting.enriched_query_history. Please use a different label name.",
    ),
    (
        "call ADMIN.CREATE_LABEL('QUERY_TEXT', NULL, NULL, 'compilation_time > 5000');",
        "Label name can not be same as column name in view reporting.enriched_query_history. Please use a different label name.",
    ),
    (
        "call ADMIN.UPDATE_LABEL('{label}', NULL, NULL, NULL, 'compile_time > 5000');",
        "Name must not be null.",
    ),
    (
        "call ADMIN.UPDATE_LABEL('{label}', '{label}_1', NULL, 100, 'compilation_time > 5000');",
        "Rank must only be set if group name is also provided.",
    ),
    (
        "call ADMIN.UPDATE_LABEL('{label}', '{label}_1', 'group_1', NULL, 'compilation_time > 5000');",
        "Rank must provided if you are creating a grouped label.",
    ),
    (
        "call ADMIN.UPDATE_LABEL('{label}', '{label}', 'group_1', 100, 'compilation_time > 5000');",
        "Label not found. Please refresh your page to see latest list of labels.",
    ),
    (
        "call ADMIN.UPDATE_LABEL('{label}', '{label}', 'group_1', 100, 'compile_time > 5000');",
        "Invalid condition SQL. Please check your syntax.",
    ),
]

# Test that validates that correct error message was returned
@pytest.mark.parametrize("statement, expected_error", test_cases)
def test_error_message(conn_cnx, timestamp_string, statement, expected_error):

    label = generate_unique_name("label", timestamp_string)
    sql = statement.format(label=label)
    assert expected_error in str(
        run_proc(conn_cnx, sql)
    ), "Stored procedure output does not match expected result!"


# Test that validates that we get correct error on attempt to create label with existing name
def test_create_label_with_existing_name(conn_cnx, timestamp_string):

    label = generate_unique_name("label", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'compilation_time > 5000');"
    assert (
        run_proc(conn_cnx, sql)[0] is None
    ), "Stored procedure did not return NULL value!"

    sql = f"call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'compilation_time > 5000');"
    assert (
        run_proc(conn_cnx, sql)[0]
        == "Duplicate label name found. Please use a distinct name."
    ), "Stored procedure output does not match expected result!"


# Test that validates that we get correct errors on attempt to update existing label
def test_update_label_errors(conn_cnx, timestamp_string):

    # First label
    label = generate_unique_name("label", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'compilation_time > 5000');"
    assert (
        run_proc(conn_cnx, sql)[0] is None
    ), "Stored procedure did not return NULL value!"

    # Second label
    sql = (
        f"call ADMIN.CREATE_LABEL('{label}_2', NULL, NULL, 'compilation_time > 5000');"
    )
    assert (
        run_proc(conn_cnx, sql)[0] is None
    ), "Stored procedure did not return NULL value!"

    # Update second label with the name of the first label
    sql = f"call ADMIN.UPDATE_LABEL('{label}_2', '{label}', NULL, NULL, 'rows_produced > 100');"
    assert (
        run_proc(conn_cnx, sql)[0]
        == "A label with this name already exists. Please choose a distinct name."
    ), "Stored procedure output does not match expected result!"

    # Update label with the column name from reporting.enriched_query_history

    sql = f"call ADMIN.UPDATE_LABEL('{label}_2', 'QUERY_TEXT', NULL, NULL, 'rows_produced > 100');"
    assert (
        run_proc(conn_cnx, sql)[0]
        == "Label name can not be same as column name in view reporting.enriched_query_history. Please use a different label name."
    ), "Stored procedure output does not match expected result!"

    sql = f"call ADMIN.UPDATE_LABEL('{label}_2', 'QUERY_TEXT', 'group_1', 100, 'rows_produced > 100');"
    assert (
        run_proc(conn_cnx, sql)[0]
        == "Label name can not be same as column name in view reporting.enriched_query_history. Please use a different label name."
    ), "Stored procedure output does not match expected result!"
