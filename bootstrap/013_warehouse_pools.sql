
CREATE OR REPLACE PROCEDURE ADMIN.CREATE_WAREHOUSE_POOL(name text, max_concurrent_credits int, allowed_roles array, snowflake_tag object, target_label text)
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
from crud.warehouse_pools import Warehouse, WarehousePools
def create_warehouse_pool(bare_session, pool_name: str, max_concurrent_credits: int, allowed_roles: list[str], snowflake_tag: dict, target_label: str):
    warehouses = []
    dataframe = compute_set_of_warehouses('Medium', max_concurrent_credits)
    # TODO: get random number, secrets/random package wasn't working
    rand_prefix = 123458
    for row in dataframe.itertuples():
        # TODO: this can be parallelized
        rand_prefix += 1
        # TODO: get random number, secrets wasn't working'
        wh_name = f'SD_{pool_name}_{rand_prefix}'
        wh_size = row.warehouse_size
        autoscale_min = 1
        autoscale_max = row.num_clusters
        bare_session.call("INTERNAL.CREATE_WAREHOUSE", wh_name, wh_size, autoscale_min, autoscale_max,
                     snowflake_tag)
        for role in allowed_roles:
            bare_session.sql("GRANT USAGE ON WAREHOUSE ? TO ROLE ?", wh_name, role)

        warehouses.append(Warehouse(name=wh_name, size=wh_size, autoscale_min=autoscale_min,
                                    autoscale_max=autoscale_max))

    new_pool = WarehousePools.parse_obj(dict(
            name=pool_name,
            warehouses=warehouses,
        ))
    with transaction(bare_session) as session:
        # Write the new schedule
        new_pool.write(session)
$$;
