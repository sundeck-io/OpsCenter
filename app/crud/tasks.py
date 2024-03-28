from pydantic import BaseModel, validator
from typing import ClassVar, Tuple


class Task(BaseModel):
    known_tasks: ClassVar[Tuple[str]] = (
        "QUERY_HISTORY_MAINTENANCE",
        "WAREHOUSE_EVENTS_MAINTENANCE",
        "SIMPLE_DATA_EVENTS_MAINTENANCE",
        "SFUSER_MAINTENANCE",
        "USER_LIMITS_MAINTENANCE",
    )

    task_name: str

    def enable(self, session):
        session.sql(f"ALTER TASK TASKS.{self.task_name} RESUME").collect()

    def disable(self, session):
        session.sql(f"ALTER TASK TASKS.{self.task_name} SUSPEND").collect()

    @validator("task_name", allow_reuse=True)
    def validate_name(cls, value: str) -> str:
        assert isinstance(value, str), "Task name must be a string"
        # Normalize to uppercase and remove leading/trailing whitespace
        value = value.strip().upper()
        assert (
            value in cls.known_tasks
        ), f"Unknown task {value}, known tasks are {cls.known_tasks}"
        return value
