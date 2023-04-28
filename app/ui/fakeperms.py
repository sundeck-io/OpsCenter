from enum import Enum


class _IntegrationType(Enum):
    API = 1


class ApiIntegrationProvider(Enum):
    AWS = 1,
    AZURE = 2,
    GOOGLE = 3


class AwsGateway(Enum):
    API_GATEWAY = 1,
    PRIVATE_API_GATEWAY = 2,
    GOV_API_GATEWAY = 3,
    GOV_PRIVATE_API_GATEWAY = 4


def request_account_privileges(privileges: [str]):
    return False


def request_reference(reference: str):
    return False


def request_aws_api_integration(id: str, allowed_prefixes: [str], gateway: AwsGateway,
                                aws_role_arn: str,
                                api_key: str = None, name: str = None,
                                comment: str = None):
    return False


def request_azure_api_integration(id: str, allowed_prefixes: [str], tenant_id: str, application_id: str,
                                  api_key: str = None, name: str = None,
                                  comment: str = None):
    return False


def request_google_api_integration(id: str, allowed_prefixes: [str], audience: str, name: str = None,
                                   comment: str = None):
    return False


def request_share(share_name: str, db_name: str, db_role: str, accounts: [str]):
    return False


def get_held_account_privileges(privilege_names: [str]):
    return False


def get_missing_account_privileges(privilege_names: [str]):
    return False


def get_reference_associations(reference_name: str):
    return False
