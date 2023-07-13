import getopt
import time
import helpers
import sys


def cypress_setup(profile: str, schema: str):
    """
    Run SQl to finish app setup
    """
    conn = helpers.connect_to_snowflake(profile=profile, schema=schema)
    cur = conn.cursor()

    cur.execute(
        f"""
    BEGIN
        call {conn.database}.internal.refresh_users();
        call {conn.database}.internal.refresh_warehouse_events(true);
        call {conn.database}.internal.refresh_queries(true);
    END;
    """
    )

    start_time = time.time()

    try:
        while True:
            cur = conn.cursor()

            # Execute a query to fetch data from the table
            cur.execute(
                "SELECT * FROM internal.config where key in ('WAREHOUSE_EVENTS_MAINTENANCE', 'QUERY_HISTORY_MAINTENANCE') and value is not null;"
            )

            rows = cur.fetchall()

            # if we have two rows, means materialization is complete
            if len(rows) == 2:
                print("OpsCenter materialization complete, ready to run Cypress tests")
                break

            elapsed_time = time.time() - start_time
            # bail after 3 minutes
            if elapsed_time >= 300:
                print("OpsCenter materialization did not complete in 3 minutes!")
                sys.exit(1)

            # check every 20 seconds
            time.sleep(20)

    finally:
        cur.close()
        conn.close()


def usage():
    print("cypress.py -p <snowsql_profile_name>")


def main(argv):
    """
    Parse command line arguments and call devdeploy.
    """
    profile = "local_opscenter"
    schema = "PUBLIC"
    opts, args = getopt.getopt(argv, "hp:", ["profile="])
    for opt, arg in opts:
        if opt == "-h":
            usage()
            sys.exit()
        elif opt in ("-p", "--profile"):
            profile = arg

    if profile is None:
        usage()
        sys.exit(1)

    cypress_setup(profile, schema)


if __name__ == "__main__":
    main(sys.argv[1:])
