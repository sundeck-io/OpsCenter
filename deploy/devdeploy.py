import os
import getopt
import time
import helpers
import re
import shutil
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
    # Writes all dependencies into `@stage/python`
    for file in ["sqlglot.zip", "crud.zip"]:
        local_file_path = f"app/python/{file}"
        stage_file_path = f"@{schema}.{stage}/python"
        put_cmd = f"PUT 'file://{local_file_path}' '{stage_file_path}' overwrite=true auto_compress=false"
        print(put_cmd)
        cur.execute(put_cmd)

    # Also copy the CRUD zip into @stage/ui because the streamlit app cannot reach "out" of the `ui` directory
    # to dynamically load this from elsewhere in the stage.
    put_cmd = f"PUT 'file://app/python/crud.zip' '@{schema}.{stage}/ui' overwrite=true auto_compress=false"
    print(put_cmd)
    cur.execute(put_cmd)

    # Copy the CRUD zip from app/python to app/ui to keep devdeploy and deploy streamlit consistent with each other.
    shutil.copy2("app/python/crud.zip", "app/ui/crud.zip")


def _copy_opscenter_files(cur, schema: str, stage: str, deployment: str):
    print(f"Copying OpsCenter files to @{schema}.{stage}.")
    scripts = helpers.generate_body(False, stage_name=f"@{schema}.{stage}")
    scripts += helpers.generate_qtag()
    scripts += helpers.generate_get_sundeck_deployment_function(deployment)
    if os.path.exists("proprietary/bootstrap"):
        setup_directory = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "proprietary", "bootstrap")
        )
        scripts += helpers.generate_body(
            False, stage_name=f"@{schema}.{stage}", setup_directory=setup_directory
        )
    body = helpers.generate_setup_script(scripts)
    regex = re.compile("APPLICATION\\s+ROLE", re.IGNORECASE)
    body = regex.sub("DATABASE ROLE", body)
    regex = re.compile("OR\\s+ALTER\\s+VERSIONED\\s+SCHEMA", re.IGNORECASE)
    body = regex.sub("SCHEMA IF NOT EXISTS", body)
    cur.execute(body)


def _fake_app_package_objects(cur, database: str):
    # In the "local" mode, we don't have a real application package and the sharing model does not apply.
    # Create a fake table so the setup script can be run normally.
    filename = "deploy/devdeploy_sundeck_sharing.sql"
    _run_script(cur, filename, {"DATABASE": database})


def _limit_backfill(cur, database: str):
    filename = "deploy/devdeploy_backfill.sql"
    _run_script(cur, filename, {"DATABASE": database})


def _run_script(cur, filename, tmpl_args):
    with open(filename, "r") as f:
        tmpl = f.read()
    sql = tmpl.format(**tmpl_args)
    cur.execute(sql)


def _finish_local_setup(cur, database: str, schema: str):
    print("Setting up internal state to mimic a set-up app.")

    # Call FINALIZE_SETUP first to perform any migrations. This implicitly triggers the tasks.
    cur.execute(f"call {database}.ADMIN.FINALIZE_SETUP();")

    start_time = time.time()

    # Then, wait for the tasks to report that they have run.
    while True:
        # Execute a query to fetch data from the table
        cur.execute(
            "SELECT * FROM internal.config where key in ('WAREHOUSE_EVENTS_MAINTENANCE', 'QUERY_HISTORY_MAINTENANCE', 'SNOWFLAKE_USER_MAINTENANCE') and value is not null;"
        )

        rows = cur.fetchall()

        # if we have two rows, means materialization is complete
        if len(rows) == 3:
            print("OpsCenter setup complete.")
            break

        elapsed_time = time.time() - start_time
        # bail after 3 minutes
        if elapsed_time >= 300:
            print("Aborting OpsCenter setup as it did not complete in 3 minutes!")
            sys.exit(1)

        # check every 20 seconds
        time.sleep(20)


def devdeploy(
    profile: str, schema: str, stage: str, deployment: str, finishSetup: bool
):
    """
    Create the app package to enable local development
    :param profile: the Snowsql configuration profile to use.
    """
    conn = helpers.connect_to_snowflake(profile=profile, schema=schema)
    cur = conn.cursor()
    cur.execute("SET DEPLOYENV='DEV';")

    # Create the database (and stage) if not already present
    _setup_database(cur, conn.database, conn.schema, stage)

    # Build a new zip file with the CRUD python project.
    helpers.zip_python_module("crud", "app/crud", "app/python/crud.zip")
    if os.path.exists("proprietary"):
        helpers.zip_python_module("ml", "proprietary/ml", "app/python/ml.zip")

    # Copy dependencies into the stage
    _copy_dependencies(cur, conn.schema, stage)

    # The setup script relies on objects in the app package, but this mode of deploy does not use an app package.
    # Create those resources by hand with dummy data.
    _fake_app_package_objects(cur, conn.database)

    # Deploy the OpsCenter code into the stage.
    _copy_opscenter_files(cur, conn.schema, stage, deployment)

    # Limit the backfill to 1 day
    _limit_backfill(cur, conn.database)

    # Finish local setup by setting internal state to mimic a set-up app
    # (e.g. materializes data, starts tasks)
    if finishSetup:
        _finish_local_setup(cur, conn.database, conn.schema)

    conn.close()


def usage():
    print("devdeploy.py -p <snowsql_profile_name> -d <sundeck_deployment> -s")


def main(argv):
    """
    Parse command line arguments and call devdeploy.
    """
    profile = "local_opscenter"
    schema = "PUBLIC"
    stage = "OC_STAGE"
    deployment = "dev"
    finishSetup = True
    opts, args = getopt.getopt(
        argv, "d:hp:s", ["deployment=", "profile=", "skip-finish-setup"]
    )
    for opt, arg in opts:
        if opt == "-h":
            usage()
            sys.exit()
        elif opt in ("-p", "--profile"):
            profile = arg
        elif opt in ("-d", "--deployment"):
            deployment = arg
        elif opt in ("-s", "--skip-finish-setup"):
            finishSetup = False

    if profile is None or stage is None:
        usage()
        sys.exit()

    devdeploy(profile, schema, stage, deployment, finishSetup)


if __name__ == "__main__":
    main(sys.argv[1:])
