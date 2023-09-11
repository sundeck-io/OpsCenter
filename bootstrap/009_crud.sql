
CREATE OR REPLACE PROCEDURE ADMIN.CREATE_ENTITY(entity object, table_name text, validation_proc text)
RETURNS text
LANGUAGE python
RUNTIME_VERSION = "3.10"
HANDLER = 'create_entity'
PACKAGES = ('snowflake-snowpark-python')
imports=('{{stage}}/crud/__init__.py')
AS
$$
from crud.__init__ import create_entity
$$;
