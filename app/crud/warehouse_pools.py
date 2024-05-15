import secrets

from pydantic import validator

from typing import ClassVar, List, Optional
from .base import BaseOpsCenterModel, transaction
from snowflake.snowpark import Row
import re


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
    max_concurrent_credits: int
    roles: List[str]
    default_warehouse_size: str
    target_label: Optional[str] = None

    def get_id_col(self) -> str:
        return "name"

    def get_id(self) -> str:
        return self.name

    def to_row(self) -> Row:
        return Row(
            **dict(
                name=self.name,
                warehouses=[w.to_row() for w in self.warehouses],
                max_concurrent_credits=self.max_concurrent_credits,
                roles=self.roles,
                default_warehouse_size=self.default_warehouse_size,
                target_label=self.target_label,
            )
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
        assert is_unquoted_identifier(v) is True
        return v

    @validator("warehouses", allow_reuse=True)
    def verify_warehouses(cls, v):
        if not v:
            raise ValueError("Warehouses is required")
        assert isinstance(v, list) and all(isinstance(i, Warehouse) for i in v)
        return v

    @validator("max_concurrent_credits", allow_reuse=True)
    def verify_max_concurrent_credits(cls, v):
        if not v:
            raise ValueError("max_concurrent_credits is required")
        assert isinstance(v, int)
        return v

    @validator("roles", allow_reuse=True)
    def verify_roles(cls, v):
        if not v:
            raise ValueError("roles is required")
        assert isinstance(v, list) and all(isinstance(i, str) for i in v)
        return v

    @validator("default_warehouse_size", allow_reuse=True)
    def verify_default_warehouse_size(cls, v):
        if not v:
            raise ValueError("default_warehouse_size is required")
        assert isinstance(v, str)
        return v


def is_unquoted_identifier(string):
    # Regular expression pattern for unquoted identifier
    pattern = r"^[a-zA-Z_][a-zA-Z0-9_$]*$"

    # Check if the string matches the pattern
    return bool(re.match(pattern, string))


def get_warehouse_name(pool_name: str):
    return "_".join(["SD", pool_name, secrets.token_hex(4)])
