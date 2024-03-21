import logging
import config
import connection

try:
    from snowflake import telemetry
except ImportError:
    import faketelemetry as telemetry

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
try:
    import snowflake.permissions as perms
except ImportError:
    import fakeperms as perms


def setup_permissions():
    db = connection.execute("select current_database() as db").values[0][0]
    logger.debug(f"Setting up permissions for {db}")

    privileges = [
        "EXECUTE MANAGED TASK",
        "EXECUTE TASK",
        "MANAGE WAREHOUSES",
        "IMPORTED PRIVILEGES ON SNOWFLAKE DB",
    ]
    missing_privileges = perms.get_missing_account_privileges(privileges)
    logger.debug(f"Missing privileges: {missing_privileges}")
    if len(missing_privileges) > 0:
        telemetry.add_event(
            "setup_permissions", {"missing_privileges": missing_privileges}
        )
        logger.debug("Requesting privileges")
        res = perms.request_account_privileges(missing_privileges)
        logger.debug(f"Request result: {res}")
    else:
        logger.debug("No privileges to request")
        if not config.up_to_date():
            logger.debug("Updating config")
            expected, current = config.get_current_and_expected_version()
            telemetry.add_event(
                "setup_permissions",
                {
                    "config_updated": True,
                    "expected_version": expected,
                    "current_version": current,
                },
            )
            telemetry.set_span_attribute("config_updated", True)
            telemetry.set_span_attribute("expected_version", expected)
            telemetry.set_span_attribute("current_version", current)
            with connection.Connection.get() as conn:
                conn.call(f"{db}.ADMIN.FINALIZE_SETUP")
