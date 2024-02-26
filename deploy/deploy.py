import getopt
import os
import sys
import tempfile
from typing import Union

import helpers

DATABASE = os.environ.get("OPSCENTER_DATABASE", "DEV")
SCHEMA = "CODE"
APPLICATION_PACKAGE = os.environ.get("OPSCENTER_PACKAGE", "OPSCENTER")
APPLICATION = os.environ.get("OPSCENTER_APP", "OPSCENTER_APP")
STAGE = "SD"
FULL_STAGE = f"@{DATABASE}.{SCHEMA}.{STAGE}"
FULL_STAGE_SLASH = FULL_STAGE + "/"


def _setup_working_database(cur):
    print("Setting up base database.")
    # Create the database, schema, and stage
    cur.execute(
        f"""
    BEGIN
        CREATE DATABASE IF NOT EXISTS {DATABASE};
        USE DATABASE {DATABASE};
        CREATE SCHEMA IF NOT EXISTS {SCHEMA};
        USE SCHEMA {SCHEMA};
        CREATE STAGE IF NOT EXISTS {DATABASE}.{SCHEMA}.{STAGE}
            file_format = (type = 'CSV' field_delimiter = None record_delimiter = None);
    END;
    """
    )


def _drop_working_database(cur):
    print("Dropping base database.")
    # Create the database, schema, and stage
    cur.execute(
        f"""
    BEGIN
        DROP DATABASE IF EXISTS {DATABASE};
        DROP APPLICATION PACKAGE IF EXISTS "{APPLICATION_PACKAGE}";
    END;
    """
    )


def _clear_stage(cur):
    print("Clearing old files from stage.")
    cur.execute(f"REMOVE {FULL_STAGE} pattern='.*';")


testing_pages = {}


def _sync_local_to_stage(cur):
    print("Syncing local files to stage.")

    # Build the CRUD module as a zip file
    helpers.zip_python_module("crud", "app/crud", "app/python/crud.zip")

    # Copy the CRUD module to the stage.
    cmds = [
        # Write the CRUD zip to /python for the SQL procs
        f"PUT 'file://app/python/crud.zip' '{FULL_STAGE_SLASH}/python' overwrite=true auto_compress=false",
        # Write it into /ui, too, because streamlit can't load from outside the app's directory
        f"PUT 'file://app/python/crud.zip' '{FULL_STAGE_SLASH}/ui' overwrite=true auto_compress=false",
    ]
    for cmd in cmds:
        print(f"Running {cmd}")
        cur.execute(cmd)

    # Walk the target directory and upload all files to the stage
    target_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "app"))
    for root, dirs, files in os.walk(target_dir, followlinks=False):

        # Skip over files/directories that start with a period
        files = [f for f in files if not f.startswith(".")]
        files = [f for f in files if f not in testing_pages]
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        dirs[:] = [d for d in dirs if not d == "__pycache__"]

        for file in files:
            local_file_path = os.path.join(root, file)
            stage_file_path = FULL_STAGE_SLASH + os.path.relpath(root, target_dir)
            if stage_file_path == FULL_STAGE_SLASH + ".":
                stage_file_path = FULL_STAGE_SLASH
            put_cmd = f"PUT 'file://{local_file_path}' '{stage_file_path}' overwrite=true auto_compress=false"
            cur.execute(put_cmd)


"""This script will take all the files in the setup directory"""


def _upload_combined_setup_script(cur, deployment: str):
    print("Uploading setup script.")
    with tempfile.TemporaryDirectory():

        scripts = helpers.generate_body()
        scripts += helpers.generate_qtag()
        scripts += helpers.generate_get_sundeck_deployment_function(deployment)
        sql_file_path = os.path.join("/tmp/", "setup.sql")
        with open(sql_file_path, "w") as sql_file:
            sql_file.write(helpers.generate_setup_script(scripts))

        # Upload SQL file to Snowflake and execute it
        cur.execute(
            f"PUT file://{sql_file_path} {FULL_STAGE} overwrite=true auto_compress=false"
        )


def _install_or_update_package(
    cur,
    version: Union[str, None] = None,
    install: bool = True,
    sundeck_db: str = "SUNDECK",
):
    print(
        "Updating Snowflake application package and install. Includes running bootstrap script."
    )

    if version is None:
        using_clause = f" USING '{FULL_STAGE}' DEBUG_MODE = true"
        upgrade_clause = f" USING '{FULL_STAGE}'"
        create_version_statement = "select 1;"
        add_patch_statements = "select 1;"

    else:
        using_clause = ""  # f" VERSION {version}"
        upgrade_clause = ""
        create_version_statement = f"""
        ALTER APPLICATION PACKAGE "{APPLICATION_PACKAGE}" ADD VERSION "{version}" USING '{FULL_STAGE}' LABEL='';
        ALTER APPLICATION PACKAGE "{APPLICATION_PACKAGE}" SET DEFAULT RELEASE DIRECTIVE VERSION = "{version}" PATCH = 0;
        """
        add_patch_statements = f"""
        ALTER APPLICATION PACKAGE "{APPLICATION_PACKAGE}" ADD PATCH FOR VERSION "{version}" USING '{FULL_STAGE}' LABEL='';
        let patch number := (SELECT "patch" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
        execute immediate $$ALTER APPLICATION PACKAGE "{APPLICATION_PACKAGE}" SET DEFAULT RELEASE DIRECTIVE VERSION = "{version}" PATCH = $$ || patch || $$;$$;
        """

    cur.execute(
        f"""
        BEGIN
            GRANT CREATE APPLICATION PACKAGE ON ACCOUNT TO ROLE ACCOUNTADMIN;
            SHOW APPLICATION PACKAGES;
            let pkgexists number := (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE "name" = '{APPLICATION_PACKAGE}');
            IF (pkgexists = 0) THEN
                CREATE APPLICATION PACKAGE "{APPLICATION_PACKAGE}" COMMENT='' DISTRIBUTION = 'INTERNAL';
                {create_version_statement}
            ELSE
                {add_patch_statements}
            END IF;
        END;
        """
    )

    # Run the grants prior to upgrading the application.
    _grant_sundeck_db_access(cur, sundeck_db)

    if install:
        print(f"Creating application {APPLICATION}.")
        cur.execute(
            f"""
        BEGIN
            SHOW APPLICATIONS;
            let appexists number := (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE "name" = '{APPLICATION}');
            IF (appexists = 0) THEN
                CREATE APPLICATION "{APPLICATION}" FROM APPLICATION PACKAGE "{APPLICATION_PACKAGE}" {using_clause};
            ELSE
                ALTER APPLICATION "{APPLICATION}" UPGRADE {upgrade_clause};
            END IF;
        END;
        """
        )


def _grant_sundeck_db_access(cur, sundeck_db: str):
    print("Running grants to database SUNDECK for application package.")
    # Grant REFERENCE_USAGE on SUNDECK to the application package
    # Grant access to the view from sundeck
    cur.execute(
        f"""
    BEGIN
        CREATE SCHEMA IF NOT EXISTS "{APPLICATION_PACKAGE}".SHARING;

        CREATE DATABASE IF NOT EXISTS "{sundeck_db}";
        CREATE SCHEMA IF NOT EXISTS "{sundeck_db}".INTERNAL;
        -- TODO update these to match reality
        CREATE TABLE IF NOT EXISTS "{sundeck_db}".INTERNAL.GLOBAL_QUERY_HISTORY(sundeck_account_id text,
            snowflake_account_locator text,
            snowflake_query_id text,
            sundeck_query_id text,
            flow_name text,
            query_text_received text,
            query_text_final text,
            start_time timestamp_ntz,
            end_time timestamp_ntz,
            snowflake_submission_time timestamp_ntz,
            snowflake_end_time timestamp_ntz,
            alt_warehouse_name text,
            sundeck_status text,
            sundeck_error_code text,
            sundeck_error_message text,
            actions variant,
            is_async boolean,
            results_path text,
            request_id text,
            snowflake_region text,
            snowflake_cloud text);

        GRANT REFERENCE_USAGE ON DATABASE {sundeck_db} TO SHARE IN APPLICATION PACKAGE "{APPLICATION_PACKAGE}";

        SHOW REGIONS;
        CREATE OR REPLACE TABLE "{APPLICATION_PACKAGE}".SHARING.REGIONS(snowflake_region text, cloud text, region text) AS
            SELECT "snowflake_region", "cloud", "region" from table(result_scan(last_query_id()));

        -- Create a view in the application package that is filtered to CURRENT_ACCOUNT(). It is critical that the
        -- filtering is done here to ensure a user only sees their own history.
        CREATE OR REPLACE VIEW "{APPLICATION_PACKAGE}".SHARING.GLOBAL_QUERY_HISTORY AS SELECT * FROM {sundeck_db}.INTERNAL.GLOBAL_QUERY_HISTORY
            WHERE UPPER(SNOWFLAKE_ACCOUNT_LOCATOR) = UPPER(CURRENT_ACCOUNT()) and
            UPPER(SNOWFLAKE_REGION) = (SELECT UPPER(region) from "{APPLICATION_PACKAGE}".SHARING.REGIONS WHERE snowflake_region = SPLIT_PART(CURRENT_REGION(), '.', -1)) and
            UPPER(SNOWFLAKE_CLOUD) = (SELECT UPPER(cloud) from "{APPLICATION_PACKAGE}".SHARING.REGIONS WHERE snowflake_region = SPLIT_PART(CURRENT_REGION(), '.', -1));

        -- Grant access on the view to the application package.
        GRANT USAGE ON SCHEMA "{APPLICATION_PACKAGE}".SHARING TO SHARE IN APPLICATION PACKAGE "{APPLICATION_PACKAGE}";
        GRANT SELECT ON VIEW "{APPLICATION_PACKAGE}".SHARING.GLOBAL_QUERY_HISTORY TO SHARE IN APPLICATION PACKAGE "{APPLICATION_PACKAGE}";
        GRANT SELECT ON TABLE "{APPLICATION_PACKAGE}".SHARING.REGIONS TO SHARE IN APPLICATION PACKAGE "{APPLICATION_PACKAGE}";
    END;
    """
    )


def main(argv):
    profile = "opscenter"
    version = None
    deployment = "prod"
    skip_install = False
    skip_package = False
    opts, args = getopt.getopt(
        argv, "xhsd:p:v:", ["deployment=", "profile=", "version="]
    )
    for opt, arg in opts:
        if opt == "-h":
            print(
                "deploy.py -p <snowsql_profile_name> -s (skip install) -x (skip package creation/upload entirely) "
                + "-v <version_name>  -d <sundeck_deployment> --profile <snowsql_profile_name>, "
                + "--version <version_name> --deployment <sundeck_deployment>"
            )
            sys.exit()
        elif opt in ("-p", "--profile"):
            profile = arg
            print("==SnowSQL profile: ", profile)
        elif opt in ("-v", "--version"):
            version = arg
            print("==Version: ", version)
        elif opt == "-d":
            deployment = arg
            print("==Deployment: ", deployment)
        elif opt == "-s":
            print("==Skipping Install")
            skip_install = True
        elif opt == "-x":
            print("==Skipping Package Creation")
            skip_package = True

    execute(profile, version, deployment, not skip_install, skip_package)


def execute(
    profile: str,
    version: Union[str, None] = None,
    deployment: str = "prod",
    install: bool = True,
    skip_package: bool = False,
    sundeck_db: str = "SUNDECK",
):
    # Do Actual Work
    conn = helpers.connect_to_snowflake(profile)
    cur = conn.cursor()
    try:
        _setup_working_database(cur)
        _clear_stage(cur)
        _upload_combined_setup_script(cur, deployment)
        _sync_local_to_stage(cur)
        if not skip_package:
            _install_or_update_package(cur, version, install, sundeck_db)
        else:
            # We skip_package for the Marketplace-listing account. Make sure the grant is executed there, too.
            _grant_sundeck_db_access(cur, sundeck_db)
    finally:
        if os.environ.get("OPSCENTER_DROP_DATABASE", "false").lower() == "true":
            _drop_working_database(cur)
    conn.close()


if __name__ == "__main__":
    main(sys.argv[1:])
