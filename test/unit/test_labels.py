from __future__ import annotations

import pytest
from common_utils import (
    generate_unique_name,
    run_proc,
    fetch_objects,
)
from typing import List, Dict


def fetch_labels(conn, where_conds: str) -> List[Dict]:
    return fetch_objects(conn, "INTERNAL.LABELS", where_conds)


@pytest.mark.smoke
def test_smoke_ungrouped_label(conn, timestamp_string):
    label = generate_unique_name("label", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'rows_produced > 100');"

    # create_label returns NULL in case of successful label creation
    assert run_proc(conn, sql) is None, "ADMIN.CREATE_LABEL did not return NULL value!"

    # make sure the label was created with correct properties
    labels = fetch_labels(
        conn, f"name = '{label}' and condition = 'rows_produced > 100'"
    )
    assert len(labels) == 1, "expected 1 label"
    assert labels[0]["NAME"] == label, "label name does not match"
    assert labels[0]["CONDITION"] == "rows_produced > 100", "condition does not match"
    for k in ["GROUP_NAME", "GROUP_RANK"]:
        assert labels[0][k] is None, f"{k} should be None"
    assert labels[0]["LABEL_CREATED_AT"] is not None, "label_created_at does not match"
    orig_created_at = labels[0]["LABEL_CREATED_AT"]
    assert (
        labels[0]["LABEL_MODIFIED_AT"] is not None
    ), "label_modified_at does not match"
    orig_modified_at = labels[0]["LABEL_MODIFIED_AT"]
    assert labels[0]["ENABLED"], "enabled does not match"
    assert not labels[0]["IS_DYNAMIC"], "is_dynamic does not match"

    # update condition expression
    new_name = f"{label}_new"
    sql = f"call ADMIN.UPDATE_LABEL('{label}', '{new_name}', NULL, NULL, 'rows_produced > 1000');"
    assert run_proc(conn, sql) is None, "ADMIN.UPDATE_LABEL did not return NULL value!"

    # make sure the condition is updated
    labels = fetch_labels(
        conn, f"name = '{new_name}' and condition = 'rows_produced > 1000'"
    )
    assert len(labels) == 1, "expected 1 label"
    assert labels[0]["NAME"] == new_name, "label name does not match"
    assert labels[0]["CONDITION"] == "rows_produced > 1000", "condition does not match"
    assert (
        orig_created_at == labels[0]["LABEL_CREATED_AT"]
    ), "label_created_at should not change"
    assert (
        orig_modified_at < labels[0]["LABEL_MODIFIED_AT"]
    ), "label_modified_at should be updated"
    for k in ["GROUP_NAME", "GROUP_RANK"]:
        assert labels[0][k] is None, f"{k} should be None"

    # drop label
    sql = f"call ADMIN.DELETE_LABEL('{new_name}');"
    assert run_proc(conn, sql) is None, "ADMIN.DELETE_LABEL did not return NULL value!"
    labels = fetch_labels(conn, f"name = '{new_name}'")
    assert len(labels) == 0, f"Found label after delete {labels}"


# List of test cases with statements and expected error messages
test_cases = [
    (
        "call ADMIN.CREATE_LABEL(NULL, NULL, NULL, 'compilation_time > 5000');",
        "Name must not be null",
    ),
    (
        "call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'compile_time > 5000');",
        "Label condition failed to compile",
    ),
    (
        "call ADMIN.CREATE_LABEL('QUERY_TEXT', NULL, NULL, 'compilation_time > 5000');",
        "Label name cannot duplicate a column in REPORTING.ENRICHED_QUERY_HISTORY",
    ),
]


# Test that validates that correct error message was returned
@pytest.mark.parametrize("statement, expected_error", test_cases)
def test_label_error_message(conn, timestamp_string, statement, expected_error):
    label = generate_unique_name("label", timestamp_string)
    sql = statement.format(label=label)
    assert expected_error in str(run_proc(conn, sql))


def test_create_with_query_history_column(conn, timestamp_string):
    # Create a label with the same name as a column in ENRICHED_QUERY_HISTORY, should failed
    sql = (
        "call ADMIN.CREATE_LABEL('QUERY_TEXT', NULL, NULL, 'compilation_time > 5000');"
    )
    assert "REPORTING.ENRICHED_QUERY_HISTORY" in run_proc(
        conn, sql
    ), "Create label should fail with query history column name"


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
    assert "cannot duplicate a column" in run_proc(conn, sql)


# Test that validates that we can create/drop label with empty string for name
# Legal in Snowflake
def test_create_label_with_empty_string_name(conn):
    sql = "call ADMIN.CREATE_LABEL('', NULL, NULL, 'compilation_time > 5000');"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    sql = "call ADMIN.DELETE_LABEL('');"
    assert run_proc(conn, sql) is None


# Creates a grouped label, then tries to create an ungrouped label with the same name
def test_create_label_with_grouped_label_name(conn, timestamp_string):
    label = generate_unique_name("label", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL('{label}_1', '{label}', 10, 'compilation_time > 5000');"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    # create an ungrouped label using label_name = {label}.
    sql = f"call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'compilation_time > 5000');"
    assert "already exists" in run_proc(conn, sql)


def test_update_label_null_name(conn, timestamp_string):
    label = generate_unique_name("label", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'compilation_time > 5000');"
    assert run_proc(conn, sql) is None, "Create label failed"

    sql = f"call ADMIN.UPDATE_LABEL('{label}', NULL, NULL, NULL, 'compilation_time > 5000');"
    assert "not be null" in run_proc(
        conn, sql
    ), "Update label with null name should fail"
