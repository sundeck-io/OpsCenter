import streamlit as st
import config
import connection
from modules import add_custom_modules

if not add_custom_modules():
    st.warning("Unable to load OpsCenter modules.")


from crud import OPSCENTER_ROLE_ARN, get_api_gateway_url  # noqa E402

try:
    import snowflake.permissions as perms
except ImportError:
    import fakeperms as perms


def setup_permissions():
    db, sf_region, sd_deployment = list(
        connection.execute(
            """select current_database() as db,
        current_region() as region,
        internal.get_sundeck_deployment() as deployment"""
        ).values[0]
    )
    api_integration_name = "OPSCENTER_SUNDECK_EXTERNAL_FUNCTIONS"
    if sd_deployment != "prod":
        api_integration_name = f"OPSCENTER_SUNDECK_EXTERNAL_FUNCTIONS_{db.upper()}"
    # depending on the type of account the region may be prefixed with "public"
    # see https://docs.snowflake.com/en/sql-reference/functions/current_region
    sf_region_without_public = sf_region.split(".")[-1]

    external_func_url = get_api_gateway_url(sf_region_without_public, sd_deployment)
    with connection.Connection.get() as conn:
        conn.call("INTERNAL.SETUP_EF_URL", external_func_url)
    privileges = [
        "EXECUTE MANAGED TASK",
        "EXECUTE TASK",
        "MANAGE WAREHOUSES",
        "IMPORTED PRIVILEGES ON SNOWFLAKE DB",
    ]

    missing_privileges = perms.get_missing_account_privileges(privileges)
    if len(missing_privileges) > 0:
        perms.request_account_privileges(missing_privileges)
        # We asked for privileges but may not have gotten them. Streamlit will reload the page
        # when leaving the permissions form, so we will catch it on the second run.
        has_account_privileges = False
    else:
        has_account_privileges = True

    if len(perms.get_reference_associations("opscenter_api_integration")) == 0:
        perms.request_aws_api_integration(
            "opscenter_api_integration",
            (external_func_url,),
            perms.AwsGateway.API_GATEWAY,
            OPSCENTER_ROLE_ARN,
            None,
            api_integration_name,
            None,
        )

    if not config.up_to_date() and has_account_privileges:
        with connection.Connection.get() as conn:
            conn.call(f"{db}.ADMIN.FINALIZE_SETUP")
