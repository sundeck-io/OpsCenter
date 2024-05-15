


CREATE TABLE INTERNAL.WH_POOLS IF NOT EXISTS(
    name text,
    warehouses array,
    max_concurrent_credits int,
    roles array,
    default_warehouse_size text,
    target_label text
);

CREATE OR REPLACE PROCEDURE INTERNAL.CREATE_WAREHOUSE(wh_name string, wh_sz string, autoscale_min int, autoscale_max int, snowflake_tag object)
RETURNS object
AS
BEGIN
 -- TODO: ADD TAG
   let query string := 'CREATE OR REPLACE WAREHOUSE ' || wh_name || ' WITH WAREHOUSE_SIZE =' || '\'' || wh_sz || '\'' || ' AUTO_SUSPEND = 1' || ' MIN_CLUSTER_COUNT =' || autoscale_min || ' MAX_CLUSTER_COUNT =' || autoscale_max || '
 INITIALLY_SUSPENDED = TRUE ';
   EXECUTE IMMEDIATE :query;
   return null;
END;

CREATE OR REPLACE PROCEDURE ADMIN.CREATE_WAREHOUSE_POOL(name text, max_concurrent_credits int, default_warehouse_sz text, allowed_roles array, snowflake_tag object, target_label text)
    returns text
    language python
    runtime_version = "3.10"
    handler = 'create_warehouse_pool'
    packages = ('snowflake-snowpark-python', 'pydantic==1.*', 'snowflake-telemetry-python')
    imports = ('{{stage}}/python/crud.zip', '{{stage}}/python/ml.zip')
    EXECUTE AS OWNER
AS
$$
from ml.warehouse_pool import compute_set_of_warehouses
from crud.base import transaction
from crud.warehouse_pools import Warehouse, WarehousePools, is_unquoted_identifier, get_warehouse_name
def create_warehouse_pool(bare_session, pool_name: str, max_concurrent_credits: int, default_warehouse_sz: str, allowed_roles: list[str], snowflake_tag: dict, target_label: str):
    warehouses = []
    # check if warehouse_pool is unquoted identifier before creating any warehouse
    if not is_unquoted_identifier(pool_name):
        raise Exception("Invalid pool name")
    dataframe = compute_set_of_warehouses(default_warehouse_sz, max_concurrent_credits)
    # TODO: get random number, secrets/random package wasn't working
    rand_prefix = 123458
    for row in dataframe.itertuples():
        # TODO: this can be parallelized
        rand_prefix += 1
        wh_name = get_warehouse_name(pool_name)
        wh_size = row.warehouse_size
        autoscale_min = 1
        autoscale_max = row.num_clusters
        bare_session.call("INTERNAL.CREATE_WAREHOUSE", wh_name, wh_size, autoscale_min, autoscale_max,
                     snowflake_tag)
        for role in allowed_roles:
            role_sql = " ".join(["GRANT USAGE ON WAREHOUSE", wh_name, "TO ROLE", role])
            bare_session.sql(role_sql).collect()

        warehouses.append(Warehouse(name=wh_name, size=wh_size, autoscale_min=autoscale_min,
                                    autoscale_max=autoscale_max))

    new_pool = WarehousePools.parse_obj(dict(
            name=pool_name,
            warehouses=warehouses,
            max_concurrent_credits = max_concurrent_credits,
            roles = allowed_roles,
            default_warehouse_size = default_warehouse_sz,
            target_label = target_label
        ))
    with transaction(bare_session) as session:
        # Write the new schedule
        new_pool.write(session)
$$;
