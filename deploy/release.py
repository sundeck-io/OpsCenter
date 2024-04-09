import helpers
import os
import argparse
import sys

DATABASE = os.environ.get("OPSCENTER_DATABASE", "DEV")
SCHEMA = "CODE"
APPLICATION_PACKAGE = os.environ.get("OPSCENTER_PACKAGE", "OPSCENTER")
APPLICATION = os.environ.get("OPSCENTER_APP", "OPSCENTER_APP")
STAGE = "SD"
FULL_STAGE = f"@{DATABASE}.{SCHEMA}.{STAGE}"
FULL_STAGE_SLASH = FULL_STAGE + "/"


def main():
    parser = argparse.ArgumentParser(
        description="Release a new version of the application package"
    )
    parser.add_argument(
        "-p", "--profile", help="Snowflake profile to use", default="opscenter"
    )
    parser.add_argument(
        "-u",
        "--update",
        action="store_true",
        help="Update the application package and start a scan",
    )
    parser.add_argument(
        "-r",
        "--release",
        help="Release the application package",
        default="DEFAULT",
        nargs="*",
    )
    parser.add_argument(
        "-x", "--patch", help="Patch of the application package to release"
    )

    args = parser.parse_args()
    conn = helpers.connect_to_snowflake(args.profile)
    cur = conn.cursor()
    if args.update:
        cur.execute(
            f"ALTER APPLICATION PACKAGE {APPLICATION_PACKAGE} ADD PATCH FOR VERSION v2 USING {FULL_STAGE_SLASH}"
        )
    elif args.release:
        if len(args.release) == 1 and args.release[0] == "DEFAULT":
            cur.execute(
                f"ALTER APPLICATION PACKAGE {APPLICATION_PACKAGE} SET DEFAULT RELEASE DIRECTIVE VERSION=v2 PATCH={args.patch}"
            )
        else:
            for release in args.release:
                cur.execute(
                    f"ALTER APPLICATION PACKAGE {APPLICATION_PACKAGE} MODIFY RELEASE DIRECTIVE {release} VERSION=v2 PATCH={args.patch}"
                )
    else:
        print("No action specified")
        sys.exit(1)
    conn.close()


if __name__ == "__main__":
    main()
