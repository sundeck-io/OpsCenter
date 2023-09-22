
-- Create the WAREHOUSE_SCHEDULES table
call internal_python.create_table('WAREHOUSE_SCHEDULES');

CREATE OR REPLACE PROCEDURE ADMIN.UPDATE_WH_SCHEDULES()
    RETURNS text
    language python
    runtime_version = "3.10"
    handler = 'run'
    packages = ('snowflake-snowpark-python', 'pydantic')
    imports = ('{{stage}}/python/crud.zip')
    EXECUTE AS OWNER
 AS
$$
from crud import WarehouseSchedulesTask
def run(session):
    task = WarehouseSchedulesTask(session)
    return task.run()
$$;
