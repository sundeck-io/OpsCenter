import re
import helpers

STAGE = "PACKAGES_STAGE"
conn = helpers.connect_to_snowflake()
cur = conn.cursor()
cur.execute("SET DEPLOYENV='DEV';")
cur.execute(f"USE DATABASE {conn.database}")
cur.execute(
    f"""
        CREATE STAGE IF NOT EXISTS public.{STAGE}
            file_format = (type = 'CSV' field_delimiter = None record_delimiter = None);
            """
)
for file in ["sqlglot.zip"]:
    local_file_path = f"app/python/{file}"
    stage_file_path = f"@public.{STAGE}/python"
    put_cmd = f"PUT 'file://{local_file_path}' '{stage_file_path}' overwrite=true auto_compress=false"
    cur.execute(put_cmd)
scripts = helpers.generate_body(False, stage_name=f"@public.{STAGE}")
scripts += helpers.generate_qtag()
body = helpers.generate_setup_script(scripts)
regex = re.compile("APPLICATION\\s+ROLE", re.IGNORECASE)
body = regex.sub("DATABASE ROLE", body)
regex = re.compile("OR\\s+ALTER\\s+VERSIONED\\s+SCHEMA", re.IGNORECASE)
body = regex.sub("SCHEMA IF NOT EXISTS", body)
cur.execute(body)
conn.close()
