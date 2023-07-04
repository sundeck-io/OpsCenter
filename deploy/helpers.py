import os
from configparser import RawConfigParser
import snowflake.connector
import subprocess

DATABASE = "DEV"
SCHEMA = "CODE"
APPLICATION_PACKAGE = "Sundeck"
APPLICATION = "OPSCENTER"
STAGE = "SD"
FULL_STAGE = f"@{DATABASE}.{SCHEMA}.{STAGE}"
FULL_STAGE_SLASH = FULL_STAGE + "/"


def connect_to_snowflake(profile: str = "opscenter", schema: str = ""):
    # Define a function to remove quotes from a string
    def remove_quotes(string):
        if string.startswith('"'):
            string = string[1:]
        if string.endswith('"'):
            string = string[:-1]
        return string

    # Define the path to the SnowSQL config file
    config_path = os.path.expanduser("~/.snowsql/config")

    # Create a ConfigParser object and read the config file
    config = RawConfigParser()
    config.read(config_path)

    if not config.has_section(f"connections.{profile}"):
        raise ValueError(
            f"Profile {profile} not found in SnowSQL config file at {config_path}"
        )

    # Get the accountname, username, and password properties from the [connections] section
    accountname = remove_quotes(config.get(f"connections.{profile}", "accountname"))
    username = remove_quotes(config.get(f"connections.{profile}", "username"))
    password = remove_quotes(config.get(f"connections.{profile}", "password"))
    warehousename = remove_quotes(config.get(f"connections.{profile}", "warehousename"))
    dbname = remove_quotes(config.get(f"connections.{profile}", "dbname"))

    if len(dbname) == 0:
        raise ValueError(f"Database must be specified in config connections.{profile}")

    # Initialize the Snowflake connection
    conn = snowflake.connector.connect(
        user=username,
        password=password,
        account=accountname,
        warehousename=warehousename,
        database=dbname,
        schema=schema,
    )
    return conn


def generate_body(include_streamlit=True, stage_name=""):
    setup_directory = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "bootstrap")
    )
    files = os.listdir(setup_directory)
    files.sort()

    scripts = ""  # Read each file in the setup directory
    for filename in files:
        if not filename.endswith(".sql"):
            continue

        # Read contents of SQL file
        with open(os.path.join(setup_directory, filename), "r") as f:
            script = f.read()
        if not script.startswith("\n"):
            raise ValueError(
                f"Script must start with a newline: {filename}. This is to ensure concatenated executions "
                "use the same line numbers as direct script executions."
            )
        if filename.endswith("002_streamlit.sql") and not include_streamlit:
            continue
        filename_quoted = str(filename).replace("""'""", """\'""")
        script_quoted = (
            str(script)
            .replace("{{stage}}", stage_name)
            .replace("\\", "\\\\")
            .replace("""'""", """\\'""")
        )
        scripts += f"""
filename := '{filename_quoted}';
execute immediate 'BEGIN {script_quoted}\nEND;\n';

"""
    return scripts


def generate_setup_script(scripts: str) -> str:
    return f"""
DECLARE
  filename string := 'unknown';
BEGIN
{scripts}
EXCEPTION
  WHEN statement_error THEN
    let msg string := 'DECLARE SCRIPT_FAILED EXCEPTION (-20200,
        \\' \\nFile: ' || filename || '\\n[' || replace(SQLERRM, $$'$$, $$\\'$$) || '] was primary error. \\');
        BEGIN raise SCRIPT_FAILED; END;';
    execute immediate msg;
END;\n
    """


def generate_qtag() -> str:
    result = subprocess.run(
        "yarn install",
        shell=True,
        check=True,
        capture_output=True,
        text=True,
        cwd="qtag/js",
    )
    result.check_returncode()  # Raise an exception if the command failed
    result = subprocess.run(
        "npm run term",
        shell=True,
        check=True,
        capture_output=True,
        text=True,
        cwd="qtag/js",
    )
    result.check_returncode()  # Raise an exception if the command failed
    capture = False
    grant = False
    lines = []
    for line in result.stdout.splitlines():
        if line.startswith("-------- START OF TEMPLATE --------"):
            capture = True
            continue
        if line.startswith("-------- END OF TEMPLATE --------"):
            capture = False
            grant = True
            continue
        if grant:
            # lines.append(f'GRANT USAGE ON FUNCTION TOOLS.{line} TO APPLICATION ROLE ADMIN;')
            grant = False
        if capture:
            lines.append(line)
    script = "\n".join(lines)
    return script
