import pytest


default_settings = {
    "default_timezone": "America/Los_Angeles",
    "storage_cost": "40.0",
    "compute_credit_cost": "2.0",
    "serverless_credit_cost": "3.0",
}


def enable_task(cursor, name: str):
    row = cursor.execute(
        "call admin.enable_task(%(name)s)", params={"name": name}
    ).fetchone()
    return row[0]


def disable_task(cursor, name: str):
    row = cursor.execute(
        "call admin.disable_task(%(name)s)", params={"name": name}
    ).fetchone()
    return row[0]


def update_setting(cursor, key: str, value: str) -> str:
    row = cursor.execute(
        "call admin.update_setting(%(key)s, %(value)s)",
        params={"key": key, "value": value},
    ).fetchone()
    return row[0]


def describe_setting(cursor, key: str) -> str:
    row = cursor.execute(
        "call admin.describe_setting(%(key)s)", params={"key": key}
    ).fetchone()
    return row[0]


def reset_default_settings(cur):
    for k, v in default_settings.items():
        res = update_setting(cur, k, v)
        assert res == "", f"Saw error updating setting {k} to {v}: {res}"


@pytest.mark.parametrize(
    "key,value",
    [
        ("storage_cost", "35.5"),
        ("compute_credit_cost", "1.75"),
        ("serverless_credit_cost", "2.75"),
    ],
)
def test_update_costs(conn, key, value):
    try:
        with conn() as cnx, cnx.cursor() as cur:
            result = update_setting(cur, key, value)
            assert result == "", f"Saw error updating storage cost: {result}"

            actual_val = describe_setting(cur, key)
            assert actual_val == value
    finally:
        with conn() as cnx, cnx.cursor() as cur:
            reset_default_settings(cur)


def test_update_timezone(conn):
    try:
        with conn() as cnx, cnx.cursor() as cur:
            tz = "Europe/London"
            result = update_setting(cur, "default_timezone", tz)
            assert result == "", f"Saw error updating timezone: {result}"

            val = describe_setting(cur, "default_timezone")
            assert val == tz
    finally:
        with conn() as cnx, cnx.cursor() as cur:
            reset_default_settings(cur)


def test_update_unknown_setting(conn):
    with conn() as cnx, cnx.cursor() as cur:
        result = update_setting(cur, "something fake", "foo")
        assert result != "", "Should have seen error updating unknown setting"

        actual_val = describe_setting(cur, "something fake")
        assert actual_val is None


@pytest.mark.parametrize(
    "key,badvalue",
    [
        ("storage_cost", "foo"),
        ("default_timezone", "not a timezone"),
        ("default_timezone", "15.0"),
    ],
)
def test_update_bad_values(conn, key, badvalue):
    with conn() as cnx, cnx.cursor() as cur:
        result = update_setting(cur, key, badvalue)
        assert result != "", f"Should have seen error updating {key} with {badvalue}"


@pytest.mark.parametrize(
    "name,success",
    [
        ("QUERY_HISTORY_MAINTENANCE", True),
        ("query_history_maintenance ", True),
        ("WAREHOUSE_EVENTS_MAINTENANCE", True),
        ("SFUSER_MAINTENANCE", True),
        ("foo", False),
    ],
)
def test_task_management(conn, name: str, success: bool):
    with conn() as cnx, cnx.cursor() as cur:
        resp = enable_task(cur, name)
        if success:
            assert resp == "", "Enabling task should not return an error"
        else:
            assert resp != "", "Enabling task should return an error"

        resp = disable_task(cur, name)
        if success:
            assert resp == "", "Disabling task should not return an error"
        else:
            assert resp != "", "Disabling task should return an error"
