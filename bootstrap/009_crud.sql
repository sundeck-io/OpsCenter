
CREATE OR REPLACE PROCEDURE ADMIN.CREATE_ENTITY(table_name text, entity object, validation_proc text)
RETURNS text
LANGUAGE python
RUNTIME_VERSION = "3.10"
HANDLER = 'create_entity'
PACKAGES = ('snowflake-snowpark-python', 'pydantic')
imports=('{{stage}}/crud/__init__.py')
AS
$$
from __init__ import create_entity
$$;
