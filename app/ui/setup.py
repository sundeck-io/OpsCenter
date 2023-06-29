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

    db, account, user, sf_region = list(
        connection.execute(
            """select current_database() as db,
        current_account() as account,
        current_user() as username,
        current_region() as region"""
        ).values[0]
    )

    # depending on the type of account the region may be prefixed with "public"
    # see https://docs.snowflake.com/en/sql-reference/functions/current_region
    sf_region_without_public = sf_region.split(".")[-1]

    region = region_map[sf_region_without_public]

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
            f"""
        ### Enable Notifications (optional, via Sundeck)
        To enable OpsCenter notifications, signup for a free Sundeck account. This is optional but brings the following benefits:
        * Alerting within OpsCenter (email available now, Slack and Teams coming soon).
        * Labeling using QLike for intelligent query matching.
        * Access to Sundeck's query engineering platform (with per-query pre and post hooks).

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
                    "https://1lf9af4dk7.execute-api.us-east-1.amazonaws.com",
                    "https://mr2gl3hcuk.execute-api.us-east-2.amazonaws.com",
                    "https://1fb567sika.execute-api.us-west-2.amazonaws.com",
                    "https://rkb9hwsqw0.execute-api.us-east-1.amazonaws.com",
                    "https://hh538sr9qg.execute-api.us-west-2.amazonaws.com",
                    "https://w4cu711jd2.execute-api.us-west-2.amazonaws.com",
                ),
                perms.AwsGateway.API_GATEWAY,
                "arn:aws:iam::323365108137:role/SnowflakeOpsCenterRole",
                None,
                "OPSCENTER_SUNDECK_EXTERNAL_FUNCTIONS",
                None,
            )
            if req is None:
                msg.info("Token recorded, creating API integration.")
                config.clear_cache()
            else:
                msg.info("Please run the following command in your Snowflake account:")
                st.code(
                    """
BEGIN
  CREATE OR REPLACE API INTEGRATION OPSCENTER_SUNDECK_EXTERNAL_FUNCTIONS api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::323365108137:role/SnowflakeOpsCenterRole' """
                    + """ api_allowed_prefixes = ('https://1lf9af4dk7.execute-api.us-east-1.amazonaws.com', 'https://mr2gl3hcuk.execute-api.us-east-2.amazonaws.com', """
                    + """'https://1fb567sika.execute-api.us-west-2.amazonaws.com', 'https://rkb9hwsqw0.execute-api.us-east-1.amazonaws.com', """
                    + f"""'https://hh538sr9qg.execute-api.us-west-2.amazonaws.com', 'https://w4cu711jd2.execute-api.us-west-2.amazonaws.com') enabled = true;
  GRANT USAGE ON INTEGRATION OPSCENTER_SUNDECK_EXTERNAL_FUNCTIONS TO APPLICATION "{db}";
  CALL ADMIN.SETUP_EXTERNAL_FUNCTIONS();
END;
                """
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
