from app.crud.warehouse_pools import (
    WarehousePools,
    Warehouse,
    is_unquoted_identifier,
    get_warehouse_name,
)


def _get_warehouse_pool(
    name="pool1",
    # warehouses = [Warehouse(name="wh1", size="small", autoscale_min=1, autoscale_max=2), Warehouse(name="wh2", size="medium", autoscale_min=2, autoscale_max=4),
    warehouses=[
        Warehouse(
            name="wh3",
            size="XSMALL",
            autoscale_min=1,
            autoscale_max=1,
            snowflake_tag=None,
        )
    ],
) -> dict:
    d = dict(
        name=name,
        warehouses=warehouses,
    )
    return d


def test_basic_pool(session):
    test_wh_pool = _get_warehouse_pool(name="pool1")
    wh_pool = WarehousePools.parse_obj(test_wh_pool)
    assert wh_pool.name == "pool1"
    row = wh_pool.to_row()
    print(row)
    # write should hit failure
    wh_pool.write(session)


def test_valid_quoted_identifiers(session):
    assert is_unquoted_identifier("WH1") is True
    assert is_unquoted_identifier("wh1") is True
    assert is_unquoted_identifier("wh 1") is False
    assert is_unquoted_identifier("wh + 1") is False
    assert is_unquoted_identifier('"wh1"') is False


def test_get_warehouse_name(session):
    # 8 hex character + 2 ("SD") + 2 ("_")
    assert len(get_warehouse_name("POOL1")) == len("POOL1") + 12
