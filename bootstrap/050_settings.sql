
CREATE OR REPLACE PROCEDURE ADMIN.DESCRIBE_SETTING(name text)
    RETURNS TEXT
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
BEGIN
    let res text := '';
    call internal.get_config(:name) into :res;
    return res;
END;

CREATE OR REPLACE PROCEDURE ADMIN.UPDATE_SETTING(name TEXT, value TEXT)
    RETURNS TEXT
    LANGUAGE PYTHON
    RUNTIME_VERSION = "3.10"
    HANDLER = 'run'
    PACKAGES = ('snowflake-snowpark-python', 'pydantic')
    IMPORTS = ('{{stage}}/python/crud.zip')
    EXECUTE AS OWNER
AS
$$
from crud.base import transaction
from crud.errors import summarize_error
from crud.settings import Setting
def run(bare_session, name: str, value: str):
    with transaction(bare_session) as session:
        try:
            setting = Setting(key=name, value=value)
            setting.write(session)
            return ""
        except Exception as ve:
            return summarize_error("Failed to update setting", ve)
$$;
