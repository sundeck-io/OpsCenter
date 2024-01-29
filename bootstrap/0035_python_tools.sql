
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
