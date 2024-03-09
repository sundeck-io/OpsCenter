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
def test_smoke_grouped_label(conn, timestamp_string):
    name = generate_unique_name("label", timestamp_string)
    group_name = generate_unique_name("group", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL('{name}', '{group_name}', 50, 'query_type = \\'SELECT\\'');"

    # create_label returns NULL in case of successful label creation
    assert run_proc(conn, sql) is None, "ADMIN.CREATE_LABEL did not return NULL value!"

    # make sure the label was created with correct properties
    labels = fetch_labels(conn, f"name = '{name}' and group_name = '{group_name}'")
    assert len(labels) == 1, "expected 1 label"
    assert labels[0]["NAME"] == name, "label name does not match"
    assert labels[0]["CONDITION"] == "query_type = 'SELECT'", "condition does not match"
    assert labels[0]["GROUP_NAME"] == group_name, "group_name does not match"
    assert labels[0]["GROUP_RANK"] == 50, "group_rank does not match"
    assert labels[0]["LABEL_CREATED_AT"] is not None, "label_created_at does not match"
    orig_created_at = labels[0]["LABEL_CREATED_AT"]
    assert (
        labels[0]["LABEL_MODIFIED_AT"] is not None
    ), "label_modified_at does not match"
    orig_modified_at = labels[0]["LABEL_MODIFIED_AT"]
    assert labels[0]["ENABLED"], "enabled does not match"
    assert not labels[0]["IS_DYNAMIC"], "is_dynamic does not match"

    # update condition expression
    new_name = f"{name}_new"
    sql = f"call ADMIN.UPDATE_LABEL('{name}', '{new_name}', '{group_name}', 100, 'query_type ilike \\'select\\'');"
    assert run_proc(conn, sql) is None, "ADMIN.UPDATE_LABEL did not return NULL value!"

    labels = fetch_labels(conn, f"name = '{new_name}' and group_name = '{group_name}'")
    assert len(labels) == 1, "expected 1 label"
    assert labels[0]["NAME"] == new_name, "label name does not match"
    assert (
        labels[0]["CONDITION"] == "query_type ilike 'select'"
    ), "condition does not match"
    assert labels[0]["GROUP_NAME"] == group_name, "group_name does not match"
    assert labels[0]["GROUP_RANK"] == 100, "group_rank does not match"
    assert (
        labels[0]["LABEL_CREATED_AT"] == orig_created_at
    ), "label_created_at does not match"
    assert (
        labels[0]["LABEL_MODIFIED_AT"] > orig_modified_at
    ), "label_modified_at should have been increased"
    assert labels[0]["ENABLED"], "enabled does not match"
    assert not labels[0]["IS_DYNAMIC"], "is_dynamic does not match"

    # drop label
    sql = f"call ADMIN.DELETE_LABEL('{new_name}');"
    assert run_proc(conn, sql) is None, "ADMIN.DELETE_LABEL did not return NULL value!"
    labels = fetch_labels(conn, f"name = '{new_name}' and group_name = '{group_name}'")
    assert len(labels) == 0, f"Found label after delete {labels}"


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
        "call ADMIN.CREATE_LABEL('{label}', 'group_1', 100, 'blah_time > 5000');",
        "Label condition failed to compile",
    ),
    (
        "call ADMIN.UPDATE_LABEL('{label}', '{label}', 'group_1', 100, 'compilation_time > 5000');",
        "does not exist",
    ),
    (
        "call ADMIN.CREATE_LABEL('{label}', 'QUERY_TEXT', 10, 'compilation_time > 5000');",
        "Label group name cannot duplicate a column in REPORTING.ENRICHED_QUERY_HISTORY",
    ),
]


# Test that validates that correct error message was returned
@pytest.mark.parametrize("statement, expected_error", test_cases)
def test_error_message(conn, timestamp_string, statement, expected_error):
    label = generate_unique_name("label", timestamp_string)
    sql = statement.format(label=label)
    assert expected_error in str(run_proc(conn, sql))


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

    assert "already exists" in run_proc(conn, sql)


# Test that validates the behavior when we create ungrouped label, then grouped label
def test_update_invalid_condition(conn, timestamp_string):
    label = generate_unique_name("label", timestamp_string)
    group = generate_unique_name("group", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL('{label}', '{group}', 100, 'compilation_time > 5000');"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    # give a bad condition on update
    sql = f"call ADMIN.UPDATE_LABEL('{label}', '{label}', '{group}', 20, 'blah_time > 5000');"
    assert "failed to compile" in run_proc(conn, sql)


def test_group_name_conflicts_with_ungrouped_name(conn, timestamp_string):
    label = generate_unique_name("label", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL('{label}', NULL, NULL, 'compilation_time > 5000');"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    # grouped label has its group_name conflicting with the ungrouped label's name.
    sql = f"call ADMIN.CREATE_LABEL('{label}_1', '{label}', 20, 'compilation_time > 5000');"
    assert "already exists" in run_proc(conn, sql)


def test_group_name_cannot_duplicate_query_history_columns(conn, timestamp_string):
    label = generate_unique_name("label", timestamp_string)
    group = generate_unique_name("group", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL('{label}', '{group}', 100, 'compilation_time > 5000');"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    # On update, group name should be checked against query history columns
    sql = f"call ADMIN.UPDATE_LABEL('{label}', '{label}', 'QUERY_TEXT', 20, 'compilation_time > 5000');"
    assert "cannot duplicate a column" in run_proc(conn, sql)


def test_grouped_label_must_have_rank(conn, timestamp_string):
    label = generate_unique_name("label", timestamp_string)
    group = generate_unique_name("group", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL('{label}', '{group}', NULL, 'compilation_time > 5000');"
    res = run_proc(conn, sql)
    assert (
        "Grouped labels must have a rank" in res
    ), "Expected error message not found in response"
