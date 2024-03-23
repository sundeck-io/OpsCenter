from __future__ import annotations

import pytest
from common_utils import (
    generate_unique_name,
    run_proc,
    fetch_objects,
)
from typing import Dict, List


def fetch_labels(conn, where_conds: str) -> List[Dict]:
    return fetch_objects(conn, "INTERNAL.LABELS", where_conds)


@pytest.mark.smoke
def test_smoke_dynamic_grouped_label(conn, timestamp_string):
    name = generate_unique_name("group", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL(NULL, '{name}', NULL, 'query_type', true);"

    # create_label returns NULL in case of successful label creation
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    labels = fetch_labels(conn, f"group_name = '{name}' and is_dynamic")
    assert len(labels) == 1, "expected 1 label"
    actual = labels[0]
    assert actual["NAME"] is None, "label name does not match"
    assert actual["GROUP_NAME"] == name, "label group name does not match"
    assert actual["CONDITION"] == "query_type", "condition does not match"
    assert actual["GROUP_RANK"] is None, "rank should be none"
    assert actual["LABEL_CREATED_AT"] is not None, "label_created_at does not match"
    orig_created_at = labels[0]["LABEL_CREATED_AT"]
    assert actual["LABEL_MODIFIED_AT"] is not None, "label_modified_at does not match"
    orig_modified_at = actual["LABEL_MODIFIED_AT"]
    assert actual["ENABLED"], "enabled does not match"
    assert actual["IS_DYNAMIC"], "is_dynamic does not match"

    # update condition expression
    sql = f"call ADMIN.UPDATE_LABEL('{name}', NULL, '{name}', NULL, 'lower(query_type)', true);"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    # make sure the condition is updated
    labels = fetch_labels(conn, f"group_name = '{name}' and is_dynamic")
    assert len(labels) == 1, "expected 1 label"
    actual = labels[0]
    assert actual["CONDITION"] == "lower(query_type)", "condition does not match"
    assert (
        orig_created_at == actual["LABEL_CREATED_AT"]
    ), "label_created_at does not match"
    assert (
        orig_modified_at < actual["LABEL_MODIFIED_AT"]
    ), "label_modified_at should have increased"

    # drop label
    sql = f"call ADMIN.DELETE_DYNAMIC_LABEL('{name}');"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"


test_cases = [
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


def test_update_with_nonnull_rank(conn, timestamp_string):
    label = generate_unique_name("label", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL(NULL, '{label}', NULL, 'QUERY_TYPE', TRUE);"
    assert run_proc(conn, sql) is None, "Create dynamic label should succeed"

    sql = (
        f"call ADMIN.UPDATE_LABEL('{label}', NULL, '{label}', 10, 'QUERY_TYPE', TRUE);"
    )
    assert "Dynamic labels cannot have a rank" in run_proc(
        conn, sql
    ), "Update should fail with rank given"


def test_update_with_nonnull_name(conn, timestamp_string):
    label = generate_unique_name("label", timestamp_string)
    sql = f"call ADMIN.CREATE_LABEL(NULL, '{label}', NULL, 'QUERY_TYPE', TRUE);"
    assert run_proc(conn, sql) is None, "Create dynamic label should succeed"

    sql = f"call ADMIN.UPDATE_LABEL('{label}', 'NAME', '{label}', NULL, 'QUERY_TYPE', TRUE);"
    assert "Dynamic labels cannot have a name" in run_proc(
        conn, sql
    ), "Update should fail with name given"
