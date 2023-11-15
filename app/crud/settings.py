import pytz
from pydantic import (
    root_validator,
)
from typing import ClassVar, Union, Dict
from .base import BaseOpsCenterModel, transaction


class Setting(BaseOpsCenterModel):
    table_name: ClassVar[str] = "SETTINGS"
    # The mapping of setting key and expected type known by OpsCenter
    known_settings: ClassVar[Dict[str, type]] = {
        "default_timezone": str,
        "storage_cost": float,
        "serverless_credit_cost": float,
        "compute_credit_cost": float,
    }

    key: str
    value: Union[str, float]

    def get_id_col(self) -> str:
        return "key"

    def get_id(self) -> str:
        return self.key

    def delete(self, session):
        raise NotImplementedError("Delete is not supported for settings")

    def write(self, session):
        with transaction(session) as txn:
            txn.sql(
                "call internal.set_config(?, ?)", params=(self.key, self.value)
            ).collect()

    @root_validator(allow_reuse=True)
    @classmethod
    def validate_value(cls, values) -> "Setting":
        # Validate a key was given
        key = values.get("key", None)
        assert key, "Settings key cannot be null"
        assert key in cls.known_settings, f"Unknown setting {key}"

        # Pydantic gives us a dict[str, str], we have to check the type conversion ourselves because
        # we can't access
        v = values.get("value", None)
        assert v, "Settings value cannot be null"

        expected_type = cls.known_settings[key]
        try:
            v = expected_type(v)
        except ValueError:
            pass

        assert (
            type(v) == expected_type
        ), f"Expected {expected_type.__name__} for {key} (got {type(v).__name__})"

        if key == "default_timezone":
            # Check the timezone after we've validated that we have the expected value type
            cls.verify_timezone(v)
            assert (
                values.get("value") in pytz.all_timezones
            ), f"Unknown timezone {values.get('value')}"
        elif key in ["storage_cost", "serverless_credit_cost", "compute_credit_cost"]:
            # Make sure we have a sane cost
            cls.verify_cost(v)

        return values

    @classmethod
    def verify_timezone(cls, tz: str):
        assert tz in pytz.all_timezones, f"Unknown timezone {tz}"

    @classmethod
    def verify_cost(cls, cost: float):
        assert cost > 0, f"Credit cost must be greater than 0 (got {cost})"
