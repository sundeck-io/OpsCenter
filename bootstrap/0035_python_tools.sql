

CREATE OR REPLACE PROCEDURE INTERNAL_PYTHON.PYTHON_CENTRAL_PROC(input object, method varchar)
returns text
language python
runtime_version = '3.10'
handler = 'main'
packages = ('snowflake-snowpark-python', 'pydantic', 'snowflake-telemetry-python')
imports = ('{{stage}}/python/crud.zip')
execute as owner
as
$$
from crud import main as crud_main
def main(session, input, method):
    return crud_main(session, method, **input)
$$;

-- Creates the given table using the Python Model class
CREATE OR REPLACE PROCEDURE INTERNAL_PYTHON.CREATE_TABLE(name text)
    RETURNS text
    language python
    runtime_version = "3.10"
    handler = 'create_table'
    packages = ('snowflake-snowpark-python', 'pydantic', 'snowflake-telemetry-python')
    imports = ('{{stage}}/python/crud.zip')
    EXECUTE AS OWNER
 AS
$$
from crud import create_table
$$;
