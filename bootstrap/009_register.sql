
create table if not exists internal.external_id_table as (select randstr(16, random()) as external_id);

create or replace function internal.external_id()
returns string
language sql
as
$$
select any_value(external_id) from internal.external_id_table
$$;

CREATE OR REPLACE PROCEDURE ADMIN.REGISTER()
    RETURNS VARCHAR
    LANGUAGE python
    runtime_version="3.10"
    handler='run'
    packages = ('snowflake-snowpark-python', 'pydantic')
    imports = ('{{stage}}/python/crud.zip')
    as
$$

from crud import sundeck_signup_with_snowflake_sso

def run(session):
    db = session.sql("SELECT CURRENT_DATABASE()").to_pandas().values[0][0]
    region = session.sql("SELECT CURRENT_REGION()").to_pandas().values[0][0]
    sf_deployment = session.sql("select internal.get_sundeck_deployment()").to_pandas().values[0][0]
    return sundeck_signup_with_snowflake_sso(db, region, sf_deployment)
$$;
