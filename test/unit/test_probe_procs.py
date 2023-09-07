from __future__ import annotations

import pytest
from common_utils import generate_unique_name
from common_utils import run_proc
from common_utils import row_count


def test_smoke_create_drop_probe(conn, timestamp_string):
    probe = generate_unique_name("probe", timestamp_string)
    sql = f"CALL ADMIN.CREATE_PROBE('{probe}', 'rows_produced > 100', True :: BOOLEAN, 'SLACK', 'jinfeng@sundeck.io', 'SLACK', False :: BOOLEAN);"

    # create_probe returns NULL in case of successful probe creation
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    # make sure it was created with correct properties
    sql = f"""select count(*) from INTERNAL.probes where
                 name = '{probe}' and
                 condition = 'rows_produced > 100' and
                 notify_writer and
                 notify_writer_method = 'SLACK' and
                 notify_other = 'jinfeng@sundeck.io' and
                 notify_other_method = 'SLACK' and
                 probe_created_at is null and
                 probe_modified_at is not null and
                 not cancel and
                 enabled is null
        """
    assert row_count(conn, sql) == 1, "Probe was not found!"

    # drop probe
    sql = f"call ADMIN.DELETE_PROBE('{probe}');"
    assert run_proc(conn, sql) == "done", "Stored procedure did not return NULL value!"


# Test that validates that we get correct error on attempt to create probe with existing name
def test_create_probe_with_existing_name(conn, timestamp_string):

    probe = generate_unique_name("probe", timestamp_string)
    sql = f"CALL ADMIN.CREATE_PROBE('{probe}', 'rows_produced > 100', True :: BOOLEAN, 'SLACK', 'jinfeng@sundeck.io', 'SLACK', False :: BOOLEAN);"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    assert (
        run_proc(conn, sql)
        == "A probe with this name already exists. Please choose a distinct name."
    ), "Stored procedure output does not match expected result!"


def test_smoke_update_probe(conn, timestamp_string):
    probe = generate_unique_name("probe", timestamp_string)
    sql = f"CALL ADMIN.CREATE_PROBE('{probe}', 'compilation_time  > 50000', True :: BOOLEAN, 'EMAIL', 'doron@sundeck.io', 'EMAIL', False :: BOOLEAN);"

    # create_probe returns NULL in case of successful probe creation
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    # make sure it was created with correct properties
    sql = f"""select count(*) from INTERNAL.probes where
                 name = '{probe}' and
                 condition = 'compilation_time  > 50000' and
                 notify_writer and
                 notify_writer_method = 'EMAIL' and
                 notify_other = 'doron@sundeck.io' and
                 notify_other_method = 'EMAIL' and
                 probe_created_at is null and
                 probe_modified_at is not null and
                 not cancel and
                 enabled is null
        """
    assert row_count(conn, sql) == 1, "Label was not found!"

    # update probe
    sql = f"CALL ADMIN.UPDATE_PROBE('{probe}', '{probe}', 'rows_produced = 1000', True :: BOOLEAN, 'SLACK', 'doron@sundeck.io', 'SLACK', False :: BOOLEAN);"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    # validate that probe was updated correctly
    sql = f"""select count(*) from INTERNAL.probes where
                 name = '{probe}' and
                 condition = 'rows_produced = 1000' and
                 notify_writer and
                 notify_writer_method = 'SLACK' and
                 notify_other = 'doron@sundeck.io' and
                 notify_other_method = 'SLACK' and
                 probe_created_at is null and
                 probe_modified_at is not null and
                 not cancel and
                 enabled is null
        """
    assert row_count(conn, sql) == 1, "Probe was not found!"


# Test that validates that we can create/drop probe with empty string for name
# Legal in Snowflake
def test_create_probe_with_empty_string_name(conn, timestamp_string):

    sql = "CALL ADMIN.CREATE_PROBE('', 'compilation_time  > 50000', True :: BOOLEAN, 'EMAIL', 'doron@sundeck.io', 'EMAIL', False :: BOOLEAN);"
    assert run_proc(conn, sql) is None, "Stored procedure did not return NULL value!"

    sql = "call ADMIN.DELETE_PROBE('');"
    assert "done" in str(
        run_proc(conn, sql)
    ), "Stored procedure output does not match expected result!"


# List of test cases with statements and expected error messages
test_cases = [
    (
        "CALL ADMIN.CREATE_PROBE(NULL, 'rows_produced > 100', True :: BOOLEAN, 'SLACK', 'jinfeng@sundeck.io', 'SLACK', False :: BOOLEAN);",
        "Name must not be null.",
    ),
    (
        "CALL ADMIN.CREATE_PROBE('{probe}', NULL, True :: BOOLEAN, 'SLACK', 'jinfeng@sundeck.io', 'SLACK', False :: BOOLEAN);",
        "Condition must not be null.",
    ),
    (
        "CALL ADMIN.CREATE_PROBE('{probe}', 'x=y and z is not null', True :: BOOLEAN, 'SLACK', 'jinfeng@sundeck.io', 'SLACK', False :: BOOLEAN);",
        "Invalid condition SQL. Please check your syntax.",
    ),
    (
        "CALL ADMIN.CREATE_PROBE('{probe}', 'blah blah and ', True :: BOOLEAN, 'SLACK', 'jinfeng@sundeck.io', 'SLACK', False :: BOOLEAN);",
        "Invalid condition SQL. Please check your syntax.",
    ),
    (
        "CALL ADMIN.UPDATE_PROBE('{probe}', NULL, 'compilation_time > 3000', True :: BOOLEAN, 'SLACK', 'doron@sundeck.io', 'SLACK', False :: BOOLEAN);",
        "Name must not be null.",
    ),
    (
        "CALL ADMIN.UPDATE_PROBE('{probe}', 'new_probe_name', NULL, True :: BOOLEAN, 'SLACK', 'doron@sundeck.io', 'SLACK', False :: BOOLEAN);",
        "Condition must not be null.",
    ),
    (
        "CALL ADMIN.UPDATE_PROBE('{probe}', 'new_probe_name', 'x=y and z is not null', True :: BOOLEAN, 'SLACK', 'doron@sundeck.io', 'SLACK', False :: BOOLEAN);",
        "Invalid condition SQL. Please check your syntax.",
    ),
    (
        "CALL ADMIN.UPDATE_PROBE('{probe}', 'new_probe_name', 'x=y and z is not null', True :: BOOLEAN, 'SLACK', 'doron@sundeck.io', 'SLACK', False :: BOOLEAN);",
        "Invalid condition SQL. Please check your syntax.",
    ),
]

# Test that validates that correct error message was returned
@pytest.mark.parametrize("statement, expected_error", test_cases)
def test_error_message(conn, timestamp_string, statement, expected_error):

    probe = generate_unique_name("probe", timestamp_string)
    sql = statement.format(probe=probe)
    assert expected_error in str(
        run_proc(conn, sql)
    ), "Stored procedure output does not match expected result!"

def test_new_positive():
    assert True

def test_new_negative():
    assert False
