import json
from urllib.parse import urlencode
import config
import streamlit as st
import connection
import base64

try:
    import snowflake.permissions as perms
except ImportError:
    import fakeperms as perms

API_GATEWAY_DEV_US_WEST_2 = "https://w4cu711jd2.execute-api.us-west-2.amazonaws.com"
API_GATEWAY_STAGE_US_EAST_1 = "https://rkb9hwsqw0.execute-api.us-east-1.amazonaws.com"
API_GATEWAY_STAGE_US_WEST_2 = "https://hh538sr9qg.execute-api.us-west-2.amazonaws.com"
API_GATEWAY_PROD_US_EAST_1 = "https://1lf9af4dk7.execute-api.us-east-1.amazonaws.com"
API_GATEWAY_PROD_US_EAST_2 = "https://mr2gl3hcuk.execute-api.us-east-2.amazonaws.com"
API_GATEWAY_PROD_US_WEST_2 = "https://1fb567sika.execute-api.us-west-2.amazonaws.com"

OPSCENTER_ROLE_ARN = "arn:aws:iam::323365108137:role/SnowflakeOpsCenterRole"


def decode_token(token: str):
    if token[:4] != "sndk":
        raise Exception("invalid token format")
    parts = token.replace("sndk_", "").split(".")
    if len(parts) != 2:
        raise Exception("invalid token format")
    metadata = (
        base64.urlsafe_b64decode(parts[1].encode() + b"==").decode("utf-8").split(":")
    )
    if len(metadata) != 3:
        raise Exception("invalid token format")
    url = f"https://{metadata[0]}.execute-api.{metadata[1]}.amazonaws.com/{metadata[2]}"
    return parts[0], url


def setup_block():

    db, account, user, sf_region, sd_deployment = list(
        connection.execute(
            """select current_database() as db,
        current_account() as account,
        current_user() as username,
        current_region() as region,
        internal.get_sundeck_deployment() as deployment"""
        ).values[0]
    )

    # depending on the type of account the region may be prefixed with "public"
    # see https://docs.snowflake.com/en/sql-reference/functions/current_region
    sf_region_without_public = sf_region.split(".")[-1]

    region = get_region(sf_region_without_public)
    external_func_url = get_api_gateway_url(sf_region_without_public, sd_deployment)
    connection.Connection.get().call("INTERNAL.SETUP_EF_URL", external_func_url)

    def expander(num: int, title: str, finished: bool) -> st.expander:
        c = "[Pending]"
        if finished:
            c = "[Completed]"
        return st.expander(f"Step {num}: {title} {c}", expanded=(not finished))

    with expander(1, "Grant Snowflake Privileges", config.up_to_date()):
        st.markdown(
            """
        ### Grant Snowflake Privileges
        To start using OpsCenter, you need several permissions to operate.
        This is a one-time setup. To enable this functionality, please run the following command in your Snowflake
        account:
        """
        )
        st.code(
            f"""
BEGIN -- Grant OpsCenter Permissions to Monitor Warehouses and Queries
    GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION "{db}";
    GRANT EXECUTE MANAGED TASK, EXECUTE TASK, MANAGE WAREHOUSES ON ACCOUNT TO APPLICATION "{db}";
    BEGIN SHOW WAREHOUSES; LET c1 CURSOR FOR SELECT "name" N FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())); FOR wh in c1 DO let n string := wh.N; GRANT OPERATE, """
            + f"""USAGE ON WAREHOUSE IDENTIFIER(:n) TO APPLICATION "{db}"; END FOR; END;
    CALL "{db}".ADMIN.FINALIZE_SETUP(); RETURN 'SUCCESS';
END;
"""
        )
        st.button("Refresh Status", on_click=config.refresh, key="refresh")

    with expander(
        2, "Enable Notifications (optional, via Sundeck)", config.has_sundeck()
    ):
        st.markdown(
            """
                ### Enable Notifications (optional, via Sundeck)
                To enable OpsCenter notifications, signup for a free Sundeck account. This is optional but brings the following benefits:
                * Alerting within OpsCenter (email available now, Slack and Teams coming soon).
                * Labeling using QLike for intelligent query matching.
                * Access to Sundeck's query engineering platform (with per-query pre and post hooks).
            """
        )

        st.markdown(
            """
                #### Use one of the two options to create a free Sundeck account
            """
        )

        option1, option2 = st.tabs(
            ["Option 1: Signup with Snowflake SSO", "Option 2: Signup using email-Id"]
        )
        with option1:
            sundeck_signup_with_snowflake_sso(
                db, external_func_url, sf_region_without_public, sd_deployment
            )

        with option2:
            sundeck_signup_with_email(account, user, region, db)


def sundeck_signup_with_email(account, user, region, db):

    st.markdown(
        f"""
        #### Follow these steps to enable notifications:
        1. Create a free Sundeck account: [right click here]({sndk_url(account, user, region)}) and open this link in a new tab/window.
        2. Get an API token and enter below.
        3. Approve the popup to allow OpsCenter to send notifications.
        4. Start using notifications in OpsCenter.
        """
    )
    token_input = st.text_input("Sundeck Token", key="token")
    if st.button("Enable Notifications", key="connect"):

        msg = st.empty()
        msg.warning("Connecting. Please do not navigate away from this page.")
        token, url = decode_token(token_input)
        connection.Connection.get().call("INTERNAL.SETUP_SUNDECK_TOKEN", url, token)
        req = perms.request_aws_api_integration(
            "opscenter_api_integration",
            (
                API_GATEWAY_PROD_US_EAST_1,
                API_GATEWAY_PROD_US_EAST_2,
                API_GATEWAY_PROD_US_WEST_2,
                API_GATEWAY_STAGE_US_EAST_1,
                API_GATEWAY_STAGE_US_WEST_2,
                API_GATEWAY_DEV_US_WEST_2,
            ),
            perms.AwsGateway.API_GATEWAY,
            OPSCENTER_ROLE_ARN,
            None,
            "OPSCENTER_SUNDECK_EXTERNAL_FUNCTIONS",
            None,
        )
        if req is None:
            msg.info("Token recorded, creating API integration.")
            config.clear_cache()
        else:
            msg.info("Please run the following command in your Snowflake account:")
            gateway_prefixes = (
                f"'{API_GATEWAY_PROD_US_EAST_1}', '{API_GATEWAY_PROD_US_EAST_2}', "
                + f"'{API_GATEWAY_PROD_US_WEST_2}', '{API_GATEWAY_STAGE_US_EAST_1}', "
                + f"'{API_GATEWAY_STAGE_US_WEST_2}', '{API_GATEWAY_DEV_US_WEST_2}'"
            )
            api_integration_name = "OPSCENTER_SUNDECK_EXTERNAL_FUNCTIONS_1"
            setup_func = "ADMIN.SETUP_EXTERNAL_FUNCTIONS()"
            st.code(
                generate_code_to_setup_external_func(
                    db, gateway_prefixes, api_integration_name, setup_func
                )
            )


def sndk_url(account: str, user: str, region: str) -> str:
    if not user:
        user = ""
    d = {
        "sf_account": account,
        "sf_username": user,
        "sf_region": region,
    }
    state = base64.urlsafe_b64encode(json.dumps(d).encode()).decode().replace("=", "")
    params = {
        "source": "opscenter",
        "state": state,
    }
    return "https://sundeck.io/try?" + urlencode(params)
    # return (
    #     f"https://sundeck-prod.auth.us-west-2.amazoncognito.com/oauth2/authorize?identity_provider=Google"
    #     f"&redirect_uri=https://api.sundeck.io/us-west-2/v1/signup/finish&response_type=CODE"
    #     f"&client_id=6lda9fn5faecfm816s5ihgrbsh&scope=email%20openid%20profile&state={urlencode(params)}"
    # )


def sundeck_signup_with_snowflake_sso(
    app_name: str, ef_url: str, sf_region: str, sd_deployment: str
):
    if config.has_tenant_url():
        tenant_url = config.get_tenant_url()
        st.markdown(
            f"""
                #### Sundeck account is created: [Go to my Sundeck account]({tenant_url})
            """
        )

    st.markdown(
        """
        #### Sundeck-signup using Snowflake-SSO. Follow these steps to enable notifications:
        1. Prepare to create a Sundeck account. This is needed to for step 2.

        """
    )

    if st.button("Enable Sundeck API Integration", key="create_api_integration"):
        req = perms.request_aws_api_integration(
            "opscenter_api_integration_sso",
            (ef_url,),
            perms.AwsGateway.API_GATEWAY,
            OPSCENTER_ROLE_ARN,
            None,
            "OPSCENTER_SUNDECK_EXTERNAL_FUNCTIONS_SSO",
            None,
        )
        if req is None:
            print("Successfully created API integration.")
            st.code(
                """
                "Successfully created API integration. Please run the following command in your Snowflake account"
                """
            )
        else:
            print("Please run the following command in your Snowflake account:")
            api_integration_name = "OPSCENTER_SUNDECK_EXTERNAL_FUNCTIONS_2"
            setup_func = "ADMIN.SETUP_REGISTER_TENANT_FUNC()"
            gateway_prefixes = f"'{ef_url}'"
            st.code(
                generate_code_to_setup_external_func(
                    app_name, gateway_prefixes, api_integration_name, setup_func
                )
            )

    st.markdown(
        """
        2. To create a free Sundeck account, Please run the following code in your Snowflake account.

        """
    )
    st.code(
        generate_code_to_create_sundeck_account(
            db=app_name, sf_region=sf_region, sd_deployment=sd_deployment
        )
    )
    st.button("Refresh Status", on_click=config.refresh, key="refresh2")


def generate_code_to_create_sundeck_account(
    db: str, sf_region: str, sd_deployment: str, name: str = "SUNDECK_OAUTH2"
) -> str:
    return f"""
BEGIN  -- Create security integration and Sundeck account
{generate_security_integration_code(sf_region, sd_deployment, name)}
{generate_register_tenant_code(db, name)}
END
"""


def generate_security_integration_code(
    sf_region: str, sd_deployment: str, name: str
) -> str:
    redirect_url = get_redirect_url_for_security_integration(sf_region, sd_deployment)
    return (
        f"create security integration if not exists {name} type=oauth enabled=true oauth_client=CUSTOM "
        + f"oauth_client_type='CONFIDENTIAL' oauth_redirect_uri='{redirect_url}' oauth_issue_refresh_tokens=true "
        + "oauth_refresh_token_validity=86400 pre_authorized_roles_list = ('PUBLIC'); "
    )


def generate_register_tenant_code(db: str, security_integration_name: str) -> str:
    return f"""
let oauth_info variant := (parse_json(SYSTEM$SHOW_OAUTH_CLIENT_SECRETS('{security_integration_name}')));
let tenantInfo object := {db}.tools.register_tenant(:oauth_info:OAUTH_CLIENT_ID, :oauth_info:OAUTH_CLIENT_SECRET);
CALL {db}.admin.setup_sundeck_tenant_url(:tenantInfo:sundeckTenantUrl, :tenantInfo:sundeckUdfToken);
let rs resultset := (select 'Go to Sundeck UI' As msg, :tenantInfo:sundeckTenantUrl::string as url);
return table(rs);"""


def generate_code_to_setup_external_func(
    app_name: str, gateway_prefixes: str, api_integration_name: str, setup_func: str
) -> str:
    return (
        f"""
BEGIN
    CREATE OR REPLACE API INTEGRATION {api_integration_name} api_provider = aws_api_gateway """
        + f""" api_aws_role_arn = '{OPSCENTER_ROLE_ARN}' api_allowed_prefixes = ({gateway_prefixes}) enabled = true;
    GRANT USAGE ON INTEGRATION {api_integration_name} TO APPLICATION "{app_name}";
    CALL {setup_func};
END;
"""
    )


def get_redirect_url_for_security_integration(
    sf_region: str, sd_deployment: str
) -> str:

    base_url_map = {
        "prod": "https://api.sundeck.io",
        "stage": "https://api.stage.sndk.io",
        "dev": "https://api.dev.sndk.io",
    }
    base_url = base_url_map[sd_deployment]
    sd_region = get_sundeck_region(sf_region)
    return f"{base_url}/{sd_region}/v1/login/finish"


def get_api_gateway_url(sf_region: str, sd_deployment: str) -> str:

    stage_url_map = {
        "us-east-1": API_GATEWAY_STAGE_US_EAST_1,
        "us-west-2": API_GATEWAY_STAGE_US_WEST_2,
    }

    prod_url_map = {
        "us-east-1": API_GATEWAY_PROD_US_EAST_1,
        "us-east-2": API_GATEWAY_PROD_US_EAST_2,
        "us-west-2": API_GATEWAY_PROD_US_WEST_2,
    }

    if sd_deployment == "dev":
        return API_GATEWAY_DEV_US_WEST_2
    elif sd_deployment == "prod":
        return prod_url_map[get_sundeck_region(sf_region)]
    elif sd_deployment == "stage":
        sundeck_region = get_sundeck_region(sf_region)
        if sundeck_region == "us-east-1":
            return stage_url_map[sundeck_region]
        else:
            return stage_url_map["us-west-2"]
    else:
        raise Exception("Invalid deployment")


def get_sundeck_region(sf_region: str) -> str:
    # Supported Sundeck Regions ["us-east-1", "us-east-2.aws", "us-west-2"]
    sf2sd_region_map = {
        "AWS_US_WEST_2": "us-west-2",
        "AWS_US_EAST_1": "us-east-1",
        "AWS_AP_SOUTHEAST_2": "us-west-2",
        "AWS_EU_WEST_1": "us-east-1",
        "AWS_AP_SOUTHEAST_1": "us-west-2",
        "AWS_CA_CENTRAL_1": "us-west-2",
        "AWS_EU_CENTRAL_1": "us-east-1",
        "AWS_US_EAST_2": "us-east-2",
        "AWS_AP_NORTHEAST_1": "us-west-2",
        "AWS_AP_SOUTH_1": "us-west-2",
        "AWS_EU_WEST_2": "us-east-1",
        "AWS_AP_NORTHEAST_2": "us-west-2",
        "AWS_EU_NORTH_1": "us-east-1",
        "AWS_AP_NORTHEAST_3": "us-west-2",
        "AWS_SA_EAST_1": "us-west-2",
        "AWS_EU_WEST_3": "us-east-1",
        "AWS_AP_SOUTHEAST_3": "us-west-2",
        "AWS_US_GOV_WEST_1": "us-west-2",
        "AWS_US_EAST_1_GOV": "us-east-1",
    }

    return sf2sd_region_map[sf_region]


def get_region(sf_region: str) -> str:
    # sf_region has the format AWS_<REGION>_<AZ>
    region_map = {
        "AWS_US_WEST_2": "us-west-2",
        "AWS_US_EAST_1": "us-east-1",
        "AWS_AP_SOUTHEAST_2": "ap-southeast-2",
        "AWS_EU_WEST_1": "eu-west-1",
        "AWS_AP_SOUTHEAST_1": "ap-southeast-1",
        "AWS_CA_CENTRAL_1": "ca-central-1",
        "AWS_EU_CENTRAL_1": "eu-central-1",
        "AWS_US_EAST_2": "us-east-2",
        "AWS_AP_NORTHEAST_1": "ap-northeast-1",
        "AWS_AP_SOUTH_1": "ap-south-1",
        "AWS_EU_WEST_2": "eu-west-2",
        "AWS_AP_NORTHEAST_2": "ap-northeast-2",
        "AWS_EU_NORTH_1": "eu-north-1",
        "AWS_AP_NORTHEAST_3": "ap-northeast-3",
        "AWS_SA_EAST_1": "sa-east-1",
        "AWS_EU_WEST_3": "eu-west-3",
        "AWS_AP_SOUTHEAST_3": "ap-southeast-3",
        "AWS_US_GOV_WEST_1": "us-gov-west-2",
        "AWS_US_EAST_1_GOV": "us-east-1",
    }

    return region_map[sf_region]
