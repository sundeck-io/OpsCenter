OPSCENTER_ROLE_ARN = "arn:aws:iam::323365108137:role/SnowflakeOpsCenterRole"

API_GATEWAY_DEV_US_WEST_2 = "https://w4cu711jd2.execute-api.us-west-2.amazonaws.com"
API_GATEWAY_STAGE_US_EAST_1 = "https://rkb9hwsqw0.execute-api.us-east-1.amazonaws.com"
API_GATEWAY_STAGE_US_WEST_2 = "https://hh538sr9qg.execute-api.us-west-2.amazonaws.com"
API_GATEWAY_PROD_US_EAST_1 = "https://1lf9af4dk7.execute-api.us-east-1.amazonaws.com"
API_GATEWAY_PROD_US_EAST_2 = "https://mr2gl3hcuk.execute-api.us-east-2.amazonaws.com"
API_GATEWAY_PROD_US_WEST_2 = "https://1fb567sika.execute-api.us-west-2.amazonaws.com"

API_GATEWAY_ALL = [
    API_GATEWAY_PROD_US_EAST_1,
    API_GATEWAY_PROD_US_EAST_2,
    API_GATEWAY_PROD_US_WEST_2,
    API_GATEWAY_STAGE_US_EAST_1,
    API_GATEWAY_STAGE_US_WEST_2,
    API_GATEWAY_DEV_US_WEST_2,
]


def create_user(db):
    # create application role
    code = f"""
    -- set up service account for sundeck
    let pwd varchar := (SELECT randstr(16, random()));
    create user if not exists sundeck_service_user password = :pwd;
    create role if not exists sundeck_service_role;
    grant application role {db}.sundeck_service_role to role sundeck_service_role;
    create warehouse if not exists sundeck_service_warehouse with warehouse_size = 'x-small' warehouse_type = 'standard' auto_suspend = 60 auto_resume = true initially_suspended = true;
    grant usage on warehouse sundeck_service_warehouse to role sundeck_service_role;
    grant role sundeck_service_role to user sundeck_service_user;
    call {db}.admin.register_sundeck_account('sundeck_service_user', :pwd , 'sundeck_service_role', 'sundeck_service_warehouse');
    """
    return code


def sundeck_signup_with_snowflake_sso(
    app_name: str, sf_region: str, sd_deployment: str
) -> str:

    sf_region_without_public = sf_region.split(".")[-1]

    code = """
    BEGIN
    """

    code += generate_code_to_create_sundeck_account(
        db=app_name, sf_region=sf_region_without_public, sd_deployment=sd_deployment
    )
    code += create_user(app_name)
    code += f"""
    let rs resultset := (select 'Go to Sundeck UI' as msg, value as url from {app_name}.catalog.config where key='tenant_url');
    return table(rs);
END;"""
    return code


def generate_code_to_create_sundeck_account(
    db: str, sf_region: str, sd_deployment: str
) -> str:
    # The case-sensitive security integration name
    security_integration_name = "Sundeck"
    if sd_deployment != "prod":
        security_integration_name = f"SUNDECK_OAUTH_{db.upper()}"

    # A quoted version of the name to ensure the security integration has verbatim casing
    # We cannot pass the quoted name to `generate_register_tenant_code` because the
    # SHOW_OAUTH_CLIENT_SECRETS function cannot handle the extra quotes.
    quoted_name = f'"{security_integration_name}"'

    return f"""
-- Create security integration and Sundeck account
{generate_security_integration_code(sf_region, sd_deployment, quoted_name)}
{generate_register_tenant_code(db, security_integration_name)}
"""


def generate_security_integration_code(
    sf_region: str, sd_deployment: str, name: str
) -> str:
    redirect_url = get_redirect_url_for_security_integration(sf_region, sd_deployment)
    return f"""create security integration if not exists {name}
        type=oauth
        enabled=true
        oauth_client=CUSTOM
        oauth_client_type='CONFIDENTIAL'
        oauth_redirect_uri='{redirect_url}'
        oauth_issue_refresh_tokens=true
        oauth_refresh_token_validity=86400
        pre_authorized_roles_list = ('PUBLIC'); """


def generate_register_tenant_code(db: str, security_integration_name: str) -> str:
    sfAppName = "SUNDECK_OPSCENTER_" + db.upper()
    return f"""
let oauth_info variant := (parse_json(SYSTEM$SHOW_OAUTH_CLIENT_SECRETS('{security_integration_name}')));
let tenantInfo object := {db}.admin.register_tenant('{sfAppName}', :oauth_info:OAUTH_CLIENT_ID, :oauth_info:OAUTH_CLIENT_SECRET, '{db.upper()}');
CALL {db}.admin.setup_sundeck_tenant_url(:tenantInfo:sundeckTenantUrl, :tenantInfo:sundeckUdfToken);
"""


def generate_code_to_setup_external_func(
    app_name: str, gateway_prefixes: str, api_integration_name: str, setup_func: str
) -> str:
    return (
        f"""
    -- Create API integration and grant usage to the application
    CREATE OR REPLACE API INTEGRATION {api_integration_name} api_provider = aws_api_gateway """
        + f""" api_aws_role_arn = '{OPSCENTER_ROLE_ARN}' api_allowed_prefixes = ({gateway_prefixes}) enabled = true;
    GRANT USAGE ON INTEGRATION {api_integration_name} TO APPLICATION "{app_name}";
    CALL {setup_func};
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

    baseurl = ""
    if sd_deployment == "dev":
        baseurl = API_GATEWAY_DEV_US_WEST_2
    elif sd_deployment == "prod":
        baseurl = prod_url_map[get_sundeck_region(sf_region)]
    elif sd_deployment == "stage":
        sundeck_region = get_sundeck_region(sf_region)
        if sundeck_region == "us-east-1":
            baseurl = stage_url_map[sundeck_region]
        else:
            baseurl = stage_url_map["us-west-2"]
    else:
        raise Exception("Invalid deployment")

    return f"{baseurl}/{sd_deployment}"


def get_sundeck_region(sf_region: str) -> str:
    return _region_map[sf_region]["sd_region"]


def get_region(sf_region: str) -> str:
    return _region_map[sf_region]["region"]


# sf_region has the format AWS_<REGION>_<AZ>
# Supported Sundeck Regions ["us-east-1", "us-east-2.aws", "us-west-2"]
# This is a static map of snowflake-region to {region, nearby supported sundeck-region}.
# This map is used to pick nearby sundeck-region during creation of Sundeck account
_region_map = {
    "AWS_US_WEST_2": {"region": "us-west-2", "sd_region": "us-west-2"},
    "AWS_US_EAST_1": {"region": "us-east-1", "sd_region": "us-east-1"},
    "AWS_AP_SOUTHEAST_2": {"region": "ap-southeast-2", "sd_region": "us-west-2"},
    "AWS_EU_WEST_1": {"region": "eu-west-1", "sd_region": "us-east-1"},
    "AWS_AP_SOUTHEAST_1": {"region": "ap-southeast-1", "sd_region": "us-west-2"},
    "AWS_CA_CENTRAL_1": {"region": "ca-central-1", "sd_region": "us-west-2"},
    "AWS_EU_CENTRAL_1": {"region": "eu-central-1", "sd_region": "us-east-1"},
    "AWS_US_EAST_2": {"region": "us-east-2", "sd_region": "us-east-2"},
    "AWS_AP_NORTHEAST_1": {"region": "ap-northeast-1", "sd_region": "us-west-2"},
    "AWS_AP_SOUTH_1": {"region": "ap-south-1", "sd_region": "us-west-2"},
    "AWS_EU_WEST_2": {"region": "eu-west-2", "sd_region": "us-east-1"},
    "AWS_AP_NORTHEAST_2": {"region": "ap-northeast-2", "sd_region": "us-west-2"},
    "AWS_EU_NORTH_1": {"region": "eu-north-1", "sd_region": "us-east-1"},
    "AWS_AP_NORTHEAST_3": {"region": "ap-northeast-3", "sd_region": "us-west-2"},
    "AWS_SA_EAST_1": {"region": "sa-east-1", "sd_region": "us-west-2"},
    "AWS_EU_WEST_3": {"region": "eu-west-3", "sd_region": "us-east-1"},
    "AWS_AP_SOUTHEAST_3": {"region": "ap-southeast-3", "sd_region": "us-west-2"},
    "AWS_US_GOV_WEST_1": {"region": "us-gov-west-2", "sd_region": "us-west-2"},
    "AWS_US_EAST_1_GOV": {"region": "us-east-1", "sd_region": "us-east-1"},
}
