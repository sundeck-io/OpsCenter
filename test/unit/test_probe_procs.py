from __future__ import annotations

import pytest
from common_utils import generate_unique_name
from common_utils import run_proc
from common_utils import row_count


def test_smoke_create_drop_probe(conn, timestamp_string):
    probe = generate_unique_name("probe", timestamp_string)
    sql = f"CALL ADMIN.CREATE_QUERY_MONITOR('{probe}', 'rows_produced > 100', True, 'SLACK', 'jinfeng@sundeck.io', 'SLACK', False);"

    # create_probe returns NULL in case of successful probe creation
    assert run_proc(conn, sql) is None

    # make sure it was created with correct properties
    sql = f"""select count(*) from INTERNAL.probes where
                 name = '{probe}' and
                 condition = 'rows_produced > 100' and
                 notify_writer and
                 notify_writer_method = 'SLACK' and
                 notify_other = 'jinfeng@sundeck.io' and
                 notify_other_method = 'SLACK' and
                 probe_created_at is not null and
                 probe_modified_at is not null and
                 not cancel and
                 enabled is null
        """
    assert row_count(conn, sql) == 1, "Probe was not found!"

    # drop probe
    sql = f"call ADMIN.DELETE_QUERY_MONITOR('{probe}');"
    assert run_proc(conn, sql) is None


# Test that validates that we get correct error on attempt to create probe with existing name
def test_create_probe_with_existing_name(conn, timestamp_string):

    probe = generate_unique_name("probe", timestamp_string)
    sql = f"CALL ADMIN.CREATE_QUERY_MONITOR('{probe}', 'rows_produced > 100', True, 'SLACK', 'jinfeng@sundeck.io', 'SLACK', False);"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    assert "this name already exists" in run_proc(conn, sql)


def test_smoke_update_probe(conn, timestamp_string):
    probe = generate_unique_name("probe", timestamp_string)
    sql = f"CALL ADMIN.CREATE_QUERY_MONITOR('{probe}', 'compilation_time  > 50000', TRUE, 'EMAIL', 'doron@sundeck.io', 'EMAIL', FALSE);"

    # create_probe returns NULL in case of successful probe creation
    assert run_proc(conn, sql) is None

    # make sure it was created with correct properties
    sql = f"""select count(*) from INTERNAL.probes where
                 name = '{probe}' and
                 condition = 'compilation_time  > 50000' and
                 notify_writer and
                 notify_writer_method = 'EMAIL' and
                 notify_other = 'doron@sundeck.io' and
                 notify_other_method = 'EMAIL' and
                 probe_created_at is not null and
                 probe_modified_at is not null and
                 not cancel and
                 enabled is null
        """
    assert row_count(conn, sql) == 1, "Probe was not found!"

    # update probe
    sql = f"CALL ADMIN.UPDATE_QUERY_MONITOR('{probe}', '{probe}', 'rows_produced = 1000', True, 'SLACK', 'doron@sundeck.io', 'SLACK', False);"
    assert run_proc(conn, sql) is None

    # validate that probe was updated correctly
    sql = f"""select count(*) from INTERNAL.probes where
                 name = '{probe}' and
                 condition = 'rows_produced = 1000' and
                 notify_writer and
                 notify_writer_method = 'SLACK' and
                 notify_other = 'doron@sundeck.io' and
                 notify_other_method = 'SLACK' and
                 probe_created_at is not null and
                 probe_modified_at is not null and
                 not cancel and
                 enabled is null
        """
    assert row_count(conn, sql) == 1, "Probe was not found!"

    # drop probe
    sql = f"call ADMIN.DELETE_QUERY_MONITOR('{probe}');"
    assert run_proc(conn, sql) is None


# Test that validates that we can create/drop probe with empty string for name
# Legal in Snowflake
def test_create_probe_with_empty_string_name(conn, timestamp_string):

    sql = "CALL ADMIN.CREATE_QUERY_MONITOR('', 'compilation_time  > 50000', True, 'EMAIL', 'doron@sundeck.io', 'EMAIL', False);"
    assert run_proc(conn, sql) is None

    assert run_proc(conn, "call ADMIN.DELETE_QUERY_MONITOR('');") is None


# List of test cases with statements and expected error messages
test_cases = [
    (
        "CALL ADMIN.CREATE_QUERY_MONITOR(NULL, 'rows_produced > 100', True, 'SLACK', 'jinfeng@sundeck.io', 'SLACK', False);",
        "name cannot be null",
    ),
    (
        "CALL ADMIN.CREATE_QUERY_MONITOR('{probe}', NULL, True, 'SLACK', 'jinfeng@sundeck.io', 'SLACK', False);",
        "condition cannot be null",
    ),
    (
        "CALL ADMIN.CREATE_QUERY_MONITOR('{probe}', 'x=y and z is not null', True, 'SLACK', 'jinfeng@sundeck.io', 'SLACK', False);",
        "Invalid query monitor condition",
    ),
    (
        "CALL ADMIN.CREATE_QUERY_MONITOR('{probe}', 'blah blah and ', True, 'SLACK', 'jinfeng@sundeck.io', 'SLACK', False);",
        "Invalid query monitor condition",
    ),
    (
        "CALL ADMIN.UPDATE_QUERY_MONITOR('{probe}', NULL, 'compilation_time > 3000', True, 'SLACK', 'doron@sundeck.io', 'SLACK', False);",
        "name cannot be null",
    ),
    (
        "CALL ADMIN.UPDATE_QUERY_MONITOR('{probe}', 'new_probe_name', NULL, True, 'SLACK', 'doron@sundeck.io', 'SLACK', False);",
        "condition cannot be null",
    ),
    (
        "CALL ADMIN.UPDATE_QUERY_MONITOR('{probe}', 'new_probe_name', 'x=y and z is not null', True, 'SLACK', 'doron@sundeck.io', 'SLACK', False);",
        "Invalid query monitor condition",
    ),
    (
        "CALL ADMIN.UPDATE_QUERY_MONITOR('{probe}', 'new_probe_name', 'x=y and z is not null', True, 'SLACK', 'doron@sundeck.io', 'SLACK', False);",
        "Invalid query monitor condition",
    ),
]


# Test that validates that correct error message was returned
@pytest.mark.parametrize("statement, expected_error", test_cases)
def test_error_message(conn, timestamp_string, statement, expected_error):

    probe = generate_unique_name("probe", timestamp_string)
    sql = statement.format(probe=probe)
    assert expected_error.lower() in str(run_proc(conn, sql)).lower()
