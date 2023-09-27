
-- NB. Table is created in 090_post_setup.sql because you cannot call python procs from the setup.sql

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
