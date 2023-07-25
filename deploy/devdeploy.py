import getopt
import time
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


def _copy_opscenter_files(cur, schema: str, stage: str, deployment: str):
    print(f"Copying OpsCenter files to @{schema}.{stage}.")
    scripts = helpers.generate_body(False, stage_name=f"@{schema}.{stage}")
    scripts += helpers.generate_qtag()
    scripts += helpers.generate_get_sundeck_deployment_function(deployment)
    body = helpers.generate_setup_script(scripts)
    regex = re.compile("APPLICATION\\s+ROLE", re.IGNORECASE)
    body = regex.sub("DATABASE ROLE", body)
    regex = re.compile("OR\\s+ALTER\\s+VERSIONED\\s+SCHEMA", re.IGNORECASE)
    body = regex.sub("SCHEMA IF NOT EXISTS", body)
    cur.execute(body)


def _finish_local_setup(cur, database: str, schema: str):
    print("Setting up internal state to mimic a set-up app.")

    cur.execute(
        f"""
    BEGIN
        call {database}.internal.refresh_users();
        call {database}.internal.refresh_warehouse_events(true);
        call {database}.internal.refresh_queries(true);
        call {database}.ADMIN.FINALIZE_SETUP();
    END;
    """
    )

    start_time = time.time()

    while True:
        # Execute a query to fetch data from the table
        cur.execute(
            "SELECT * FROM internal.config where key in ('WAREHOUSE_EVENTS_MAINTENANCE', 'QUERY_HISTORY_MAINTENANCE') and value is not null;"
        )

        rows = cur.fetchall()

        # if we have two rows, means materialization is complete
        if len(rows) == 2:
            print("OpsCenter setup complete.")
            break

        elapsed_time = time.time() - start_time
        # bail after 3 minutes
        if elapsed_time >= 300:
            print("Aborting OpsCenter setup as it did not complete in 3 minutes!")
            sys.exit(1)

        # check every 20 seconds
        time.sleep(20)


def devdeploy(profile: str, schema: str, stage: str, deployment: str):
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
    _copy_opscenter_files(cur, conn.schema, stage, deployment)

    # Finish local setup by setting internal state to mimic a set-up app
    # (e.g. materializes data, starts tasks)
    _finish_local_setup(cur, conn.database, conn.schema)

    conn.close()


def usage():
    print("devdeploy.py -p <snowsql_profile_name> -d <sundeck_deployment>")


def main(argv):
    """
    Parse command line arguments and call devdeploy.
    """
    profile = "local_opscenter"
    schema = "PUBLIC"
    stage = "OC_STAGE"
    deployment = "dev"
    opts, args = getopt.getopt(argv, "d:hp:", ["deployment=", "profile="])
    for opt, arg in opts:
        if opt == "-h":
            usage()
            sys.exit()
        elif opt in ("-p", "--profile"):
            profile = arg
        elif opt in ("-d", "--deployment"):
            deployment = arg

    if profile is None or stage is None:
        usage()
        sys.exit()

    devdeploy(profile, schema, stage, deployment)


if __name__ == "__main__":
    main(sys.argv[1:])
