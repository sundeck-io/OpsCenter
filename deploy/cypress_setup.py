import getopt
import helpers
import sys


def cypress(profile: str, schema: str):
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
        call {conn.database}.ADMIN.FINALIZE_SETUP();
    END;
    """
    )

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
        sys.exit()

    cypress(profile, schema)


if __name__ == "__main__":
    main(sys.argv[1:])
