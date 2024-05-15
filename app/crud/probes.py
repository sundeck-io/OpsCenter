import datetime
from enum import Enum
from pydantic import root_validator, validator
from snowflake import snowpark
from typing import ClassVar, Optional, Tuple
from .base import BaseOpsCenterModel, transaction
from .session import get_current_session


class NotificationMethod(str, Enum):
    EMAIL = "EMAIL"
    SLACK = "SLACK"


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

            assert is_unique_name, "A query monitor with this name already exists."
            super().write(txn)

        session.call(self.on_success_proc)
        return None

    def update(self, session: snowpark.Session, new_probe: "Probe") -> "Probe":
        with transaction(session) as txn:
            old_probe_exists = session.sql(
                f"SELECT COUNT(*) = 1 FROM INTERNAL.{self.table_name} WHERE name = ?",
                params=(self.name,),
            ).collect()[0][0]
            assert old_probe_exists, "Query monitor not found."

            new_name_is_unique = session.sql(
                f"SELECT COUNT(*) = 0 FROM INTERNAL.{self.table_name} WHERE name = ? and name <> ?",
                params=(
                    new_probe.name,
                    self.name,
                ),
            ).collect()[0][0]
            assert new_name_is_unique, "A query monitor with this name already exists."

            super().update(txn, new_probe)

        session.call(self.on_success_proc)
        return None

    def delete(self, session):
        with transaction(session) as txn:
            found = txn.sql(
                f"select count(*) > 0 FROM internal.{self.table_name} WHERE name = ?",
                params=(self.name,),
            ).collect()[0][0]
            if not found:
                raise Exception(f"Query Monitor '{self.name}' not found")

            super().delete(txn)
        session.call(self.on_success_proc)

        return None

    def to_row(self) -> snowpark.Row:
        as_dict = dict(self)
        # Replace the Enum values as strings
        if self.notify_writer_method is not None:
            as_dict["notify_writer_method"] = self.notify_writer_method.value
        if self.notify_other_method is not None:
            as_dict["notify_other_method"] = self.notify_other_method.value
        return snowpark.Row(**as_dict)

    @validator("name", allow_reuse=True)
    @classmethod
    def name_not_empty(cls, name: str) -> str:
        assert name is not None, "Query monitor name cannot be null"
        assert "'" not in name, "The name must not contain an apostrophe"
        return name

    @validator("condition", allow_reuse=True)
    @classmethod
    def condition_not_empty(cls, value: str) -> str:
        assert value is not None, "Query monitor condition cannot be null"
        assert value, "Query monitor condition cannot be empty"
        return value

    @root_validator(allow_reuse=True)
    @classmethod
    def validate_notifications(cls, values: dict) -> dict:
        # Only validate the notification method if that notification is enabled
        if values.get("notify_writer", False):
            method = values.get("notify_writer_method", None)
            assert method is not None, "Notify writer method must be defined"
            assert (
                method.upper() in NotificationMethod.__members__
            ), f"Unsupported notification method {method}"

        if values.get("notify_other", False):
            method = values.get("notify_other_method", None)
            assert method is not None, "Notify other method must be defined"
            assert (
                method.upper() in NotificationMethod.__members__
            ), f"Unsupported notification method {method}"

            has_no_dupes, hint = _assert_no_duplicate_destinations(
                values["notify_other"], NotificationMethod(method)
            )
            assert has_no_dupes, f"Others should not contain duplicate entries: {hint}"

        return values

    @root_validator(allow_reuse=True)
    @classmethod
    def validate_probe_condition(cls, values: dict) -> dict:
        session = get_current_session()

        condition = values.get("condition")

        try:
            _ = session.sql(
                f"select {condition} from INTERNAL.DUMMY_QUERY_HISTORY_UDTF where false",
            ).collect()
        except snowpark.exceptions.SnowparkSQLException as e:
            assert False, f"Invalid query monitor condition: {e.message}"

        return values


def _assert_no_duplicate_destinations(
    destinations: str, method: NotificationMethod
) -> Tuple[bool, str]:
    """
    Tokenizes and trims the elements of `destinations`, checking for any duplicates
    :destinations: a comma-separated string of destinations.
    :method: The NotificationMethod for the destinations.
    :return: True and the empty string there are no duplicates, else False and a user-facing "hint" message.
    """
    elems = [d.strip() for d in destinations.split(",")]
    # Email addresses are case-insensitive, whereas slack destinations are case-sensitive
    if method == NotificationMethod.EMAIL:
        elems = [e.lower() for e in elems]
    set_elems = set(elems)
    if len(elems) != len(set_elems):
        duplicates = set([d for d in elems if elems.count(d) > 1])
        return False, f"Duplicates: {', '.join(duplicates)}"

    return True, ""
