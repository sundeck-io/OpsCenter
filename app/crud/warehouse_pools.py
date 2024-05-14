from pydantic import validator

from typing import ClassVar, List
from .base import BaseOpsCenterModel, transaction
from snowflake.snowpark import Row


class Warehouse(BaseOpsCenterModel):
    name: str
    size: str
    autoscale_min: int
    autoscale_max: int

    @validator("name", allow_reuse=True)
    def verify_name(cls, v):
        if not v:
            raise ValueError("Name is required")
        assert isinstance(v, str)
        return v

    @validator("size", allow_reuse=True)
    def verify_size(cls, v):
        if not v:
            raise ValueError("Size is required")
        assert isinstance(v, str)
        return v

    @validator("autoscale_min", allow_reuse=True)
    def verify_autoscale_min(cls, v):
        if not v:
            raise ValueError("autoscale_min is required")
        assert isinstance(v, int)
        return v

    @validator("autoscale_max", allow_reuse=True)
    def verify_autoscale_max(cls, v):
        if not v:
            raise ValueError("autoscale_max is required")
        assert isinstance(v, int)
        return v


class WarehousePools(BaseOpsCenterModel):
    table_name: ClassVar[str] = "WH_POOLS"
    name: str
    warehouses: List[Warehouse]

    def get_id_col(self) -> str:
        return "name"

    def get_id(self) -> str:
        return self.name

    def to_row(self) -> Row:
        return Row(
            **dict(name=self.name, warehouses=[w.to_row() for w in self.warehouses])
        )

    def delete(self, session):
        with transaction(session) as txn:
            super().delete(txn)

    def write(self, session):
        with transaction(session) as txn:
            count = txn.sql(
                """
                SELECT COUNT(*) FROM INTERNAL.WH_POOLS
                WHERE NAME = ?""",
                params=(self.name,),
            ).collect()[0][0]
            assert count == 0, "A warehouse pool with this name already exists."
            super().write(txn)

    @validator("name", allow_reuse=True)
    def verify_name(cls, v):
        if not v:
            raise ValueError("Name is required")
        assert isinstance(v, str)
        return v

    @validator("warehouses", allow_reuse=True)
    def verify_warehouses(cls, v):
        if not v:
            raise ValueError("Warehouses is required")
        assert isinstance(v, list) and all(isinstance(i, Warehouse) for i in v)
        return v
