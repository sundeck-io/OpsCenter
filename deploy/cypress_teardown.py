import getopt
import helpers
import sys


def teardown(profile: str):
    conn = helpers.connect_to_snowflake(profile=profile)
    conn.cursor().execute(f"DROP DATABASE {conn.database};")


def usage():
    print("cypress_teardown.py -p <snowsql_profile_name>")


def main(argv):
    """
    Parse command line arguments and call devdeploy.
    """
    profile = "local_opscenter"
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

    teardown(profile)


if __name__ == "__main__":
    main(sys.argv[1:])
