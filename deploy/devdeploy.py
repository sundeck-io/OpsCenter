import getopt
import helpers
import re
import sys


def _setup_database(cur, database: str, schema: str, stage: str):
    print(f"Setting up database '{database}' for local development.")
    cur.execute(
        f"""
    BEGIN
        CREATE DATABASE IF NOT EXISTS {database};
        USE DATABASE {database};
        CREATE SCHEMA IF NOT EXISTS {schema};
        USE SCHEMA {schema};
        CREATE STAGE IF NOT EXISTS {database}.{schema}.{stage}
            file_format = (type = 'CSV' field_delimiter = None record_delimiter = None);
    END;
    """
    )


def _copy_dependencies(cur, schema: str, stage: str):
    print(f"Copying dependencies to @{schema}.{stage}.")
    for file in ["sqlglot.zip"]:
        local_file_path = f"app/python/{file}"
        stage_file_path = f"@{schema}.{stage}/python"
        put_cmd = f"PUT 'file://{local_file_path}' '{stage_file_path}' overwrite=true auto_compress=false"
        cur.execute(put_cmd)


def _copy_opscenter_files(cur, schema: str, stage: str):
    print(f"Copying OpsCenter files to @{schema}.{stage}.")
    scripts = helpers.generate_body(False, stage_name=f"@{schema}.{stage}")
    scripts += helpers.generate_qtag()
    body = helpers.generate_setup_script(scripts)
    regex = re.compile("APPLICATION\\s+ROLE", re.IGNORECASE)
    body = regex.sub("DATABASE ROLE", body)
    regex = re.compile("OR\\s+ALTER\\s+VERSIONED\\s+SCHEMA", re.IGNORECASE)
    body = regex.sub("SCHEMA IF NOT EXISTS", body)
    cur.execute(body)


def devdeploy(profile: str, schema: str, stage: str):
    """
    Create the app package to enable local development
    :param profile: the Snowsql configuration profile to use.
    """
    conn = helpers.connect_to_snowflake(profile=profile, schema=schema)
    cur = conn.cursor()
    cur.execute("SET DEPLOYENV='DEV';")

    # Create the database (and stage) if not already present
    _setup_database(cur, conn.database, conn.schema, stage)

    # Copy dependencies into the stage
    _copy_dependencies(cur, conn.schema, stage)

    # Deploy the OpsCenter code into the stage.
    _copy_opscenter_files(cur, conn.schema, stage)

    conn.close()


def usage():
    print("devdeploy.py -p <snowsql_profile_name>")


def main(argv):
    """
    Parse command line arguments and call devdeploy.
    """
    profile = "local_opscenter"
    schema = "PUBLIC"
    stage = "OC_STAGE"
    opts, args = getopt.getopt(argv, "hp:", ["profile="])
    for opt, arg in opts:
        if opt == "-h":
            usage()
            sys.exit()
        elif opt in ("-p", "--profile"):
            profile = arg

    if profile is None or stage is None:
        usage()
        sys.exit()

    devdeploy(profile, schema, stage)


if __name__ == "__main__":
    main(sys.argv[1:])
