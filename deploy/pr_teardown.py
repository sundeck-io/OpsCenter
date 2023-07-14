import getopt
import helpers
import sys


def teardown(profile: str, application str):
    conn = helpers.connect_to_snowflake(profile=profile)
    conn.cursor().execute(f"DROP APPLICATION IF EXISTS {application};")


def usage():
    print("pr_teardown.py -p <snowsql_profile_name>")


def main(argv):
    profile = "local_opscenter"
    application = None
    opts, args = getopt.getopt(argv, "hpa:", ["profile=", "application="])
    for opt, arg in opts:
        if opt == "-h":
            usage()
            sys.exit()
        elif opt in ("-p", "--profile"):
            profile = arg
        elif opt in ("-a", "--application"):
            application = arg

    if profile is None or application is None:
        usage()
        sys.exit(1)

    teardown(profile, application)


if __name__ == "__main__":
    main(sys.argv[1:])
