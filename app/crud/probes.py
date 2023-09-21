
import datetime
from enum import Enum
from pydantic import root_validator, validator
from snowflake import snowpark
from typing import ClassVar, Optional
from .base import BaseOpsCenterModel, transaction
from .session import get_current_session


class NotificationMethod(str, Enum):
    EMAIL = 'EMAIL'
    SLACK = 'SLACK'


class Probe(BaseOpsCenterModel):
    table_name: ClassVar[str] = "PROBES"
    on_success_proc: ClassVar[str] = "ADMIN.UPDATE_PROBE_MONITOR_RUNNING"
    name: str
    condition: str
    notify_writer: bool = False
    notify_writer_method: Optional[NotificationMethod]
    notify_other: Optional[str] = None
    notify_other_method: Optional[NotificationMethod]
    cancel: bool = False
    enabled: Optional[bool] = None
    probe_created_at: datetime.datetime
    probe_modified_at: datetime.datetime

    # Pydantic Model Config
    class Config:
        use_enum_values: True

    def get_id_col(self) -> str:
        return "name"

    def get_id(self) -> str:
        return self.name

    def write(self, session: snowpark.Session):
        with transaction(session) as txn:
            is_unique_name = txn.sql(
                f"SELECT COUNT(*) = 0 from internal.{self.table_name} where name = ?",
                params=(self.name,),
            ).collect()[0][0]

            assert is_unique_name, "Probe name is not unique."
            super().write(txn)

        session.call(self.on_success_proc)
        return None

    def update(self, session: snowpark.Session, new_probe: "Probe") -> "Probe":
        with transaction(session) as txn:
            old_probe_exists = session.sql(
                f"SELECT COUNT(*) = 1 FROM INTERNAL.{self.table_name} WHERE name = ?",
                params=(self.name,),
            ).collect()[0][0]
            new_name_is_unique = session.sql(
                f"SELECT COUNT(*) = 0 FROM INTERNAL.{self.table_name} WHERE name = ? and name <> ?",
                params=(
                    new_probe.name,
                    self.name,
                ),
            ).collect()[0][0]
            assert (
                new_name_is_unique
            ), "Probe with this name already exists."

            assert (
                old_probe_exists
            ), "Probe not found."

            super().update(txn, new_probe)

        session.call(self.on_success_proc)
        return None

    def delete(self, session):
        super().delete(session)
        session.call(self.on_success_proc)
        return None

    @validator("name", allow_reuse=True)
    @classmethod
    def name_not_empty(cls, name: str) -> str:
        assert name, "Probe name cannot be empty"
        return name

    @validator("condition", allow_reuse=True)
    @classmethod
    def condition_not_empty(cls, value: str) -> str:
        assert value, "Probe condition cannot be empty"
        return value

    @validator("notify_writer_method", "notify_other_method", allow_reuse=True)
    @classmethod
    def notification_method_is_valid(cls, value: str) -> str:
        assert value is not None, 'Notification method must be defined'
        value = value.upper()
        assert value in NotificationMethod.__members__, "Unsupported notification method"
        return value

    @root_validator(allow_reuse=True)
    @classmethod
    def validate_notifications(cls, values: dict) -> dict:
        if values.get("notify_writer", False):
            assert values.get("notify_writer_method", None), "Notification method must be supplied"
        if values.get("notify_other", False):
            assert values.get("notify_other_method", None), "Notification method must be supplied"

        return values

    @root_validator(allow_reuse=True)
    @classmethod
    def validate_probe_condition(cls, values: dict) -> dict:
        session = get_current_session()

        name = values.get("name")
        condition = values.get("condition")

        try:
            _ = session.sql(
                f'select case when {condition} then 1 else 0 end as "{name}" from INTERNAL.DUMMY_QUERY_HISTORY_UDTF',
            ).collect()
        except snowpark.exceptions.SnowparkSQLException as e:
            assert False, f"Invalid probe condition: {e.message}"

        return values
