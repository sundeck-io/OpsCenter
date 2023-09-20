import snowflake.snowpark.exceptions
from pydantic import (
    validator,
    root_validator,
)
from typing import Optional, ClassVar, Union
import datetime
import math
import pandas as pd
from .base import BaseOpsCenterModel, transaction
from .session import get_current_session


## TODO
# copy predefined labels to labels - maybe we dont move out of sql?
class Label(BaseOpsCenterModel):
    table_name: ClassVar[str] = "LABELS"
    on_success_proc: ClassVar[str] = "INTERNAL.UPDATE_LABEL_VIEW"
    name: Optional[str] = None
    group_name: Optional[str] = None
    # Accept float to ease passing values directly from Pandas
    group_rank: Optional[Union[int,float]] = None
    label_created_at: datetime.datetime  # todo should this have a default?
    condition: str
    enabled: bool = True
    label_modified_at: datetime.datetime  # todo should this have a default?
    is_dynamic: bool = False

    def get_id_col(self) -> str:
        return "name" if self.name else "group_name"

    def get_id(self) -> str:
        return self.name if self.name else self.group_name

    def delete(self, session):
        with transaction(session) as txn:
            super().delete(txn)
        session.call(self.on_success_proc)

    def write(self, session):
        with transaction(session) as txn:
            if self.group_name:
                # check if the grouped label's name conflict with :
                #  1) another label in the same group,
                #  2) or an ungrouped label's name.
                #  3) another dynamic group name
                count = txn.sql(
                    """
                    SELECT COUNT(*) FROM INTERNAL.LABELS
                    WHERE
                        (GROUP_NAME = ? AND NAME = ? AND NAME IS NOT NULL)
                         OR (NAME = ? AND GROUP_NAME IS NULL)
                         OR (GROUP_NAME = ? AND NAME IS NULL)""",
                    params=(
                        self.group_name,
                        self.name,
                        self.group_name,
                        self.group_name,
                    ),
                ).collect()[0][0]
                assert (
                    count == 0
                ), f"A label with this name already exists."
            else:
                # check if the ungrouped label's name conflict with another ungrouped label, or a group with same name.
                count = txn.sql(
                    """
                    SELECT COUNT(*) FROM INTERNAL.LABELS
                    WHERE (NAME = ? AND GROUP_NAME IS NULL) OR (GROUP_NAME = ? AND GROUP_NAME IS NOT NULL)""",
                    params=(
                        self.name,
                        self.name,
                    ),
                ).collect()[0][0]
                assert (
                    count == 0
                ), f"A label with the name '{self.name}' already exists."

            super().write(txn)

        # re-generate the views outside the transaction
        session.call(self.on_success_proc)

    def update(self, session, obj: "Label") -> "Label":
        with transaction(session):
            if self.is_dynamic:
                old_label_exists = session.sql(
                    f"SELECT COUNT(*) FROM INTERNAL.{self.table_name} WHERE group_name = ? and is_dynamic",
                    params=(self.group_name,),
                ).collect()[0][0]
            else:
                old_label_exists = session.sql(
                    f"SELECT COUNT(*) = 1 FROM INTERNAL.{self.table_name} WHERE name = ?",
                    params=(self.name,),
                ).collect()[0][0]
                new_name_is_unique = session.sql(
                    f"SELECT COUNT(*) = 0 FROM INTERNAL.{self.table_name} WHERE name = ? and name <> ?",
                    params=(
                        obj.name,
                        self.name,
                    ),
                ).collect()[0][0]
                assert (
                    new_name_is_unique
                ), f"A label with the name '{obj.name}' already exists."

            assert (
                old_label_exists
            ), f"A label with the name '{self.name}' does not exist."

            super().update(session, obj)

        session.call(self.on_success_proc)

        return obj

    @root_validator(allow_reuse=True)
    @classmethod
    def validate_label_obj(cls, values) -> "Label":
        """
        Validates that the attributes on this Label appear valid by inspecting only the Label itself.
        """
        name = values.get("name")
        if values.get("is_dynamic"):
            assert not name, "Dynamic labels cannot have a name."
            assert not values.get(
                "group_rank"
            ), "Dynamic labels cannot have a rank."
            assert (
                values.get("group_name", None) is not None
            ), "Dynamic labels must have a group name."
        elif values.get("group_name"):
            group_name = values.get("group_name")
            assert group_name is not None, "Name must not be null."
            assert isinstance(group_name, str), "Name should be a string."
            assert values.get("group_rank"), "Grouped labels must have a rank."
        else:
            assert name is not None, "Name must not be null."
            assert isinstance(name, str), "Name must be a string."
            assert not values.get(
                "group_rank"
            ), "Group rank may only be provided for grouped labels."

        return values

    @root_validator(allow_reuse=True)
    @classmethod
    def validate_label_condition(cls, values) -> "Label":
        """
        Validates this Label against the database to check things like name uniqueness and condition validity.
        """
        session = get_current_session()
        assert session, "Session must be present"

        # Cannot check the condition if it's empty
        condition = values.get("condition")
        if condition:
            if values.get("is_dynamic"):
                stmt = f"select substring({condition}, 0, 0) from reporting.enriched_query_history where false"
            else:
                stmt = f"select case when {condition} then 1 else 0 end from reporting.enriched_query_history where false"
            try:
                session.sql(stmt).collect()
            except snowflake.snowpark.exceptions.SnowparkSQLException as e:
                assert False, f'Invalid label condition: "{e.message}".'

        return values

    @root_validator(allow_reuse=True)
    @classmethod
    def validate_label_name_against_query_history(cls, values) -> "Label":
        """
        Validates that the label's name does not duplicate a column in account_usage.query_history.
        """
        session = get_current_session()
        assert session, "Session must be present"

        # Check that the label [group] name does not conflict with any columns already in this view
        attr = "group name" if values.get("group_name") else "name"
        name = (
            values.get("group_name") if values.get("group_name") else values.get("name")
        )

        try:
            session.sql(f'select "{name}" from reporting.enriched_query_history where false').collect()
            assert (
                False
            ), f"Label {attr} cannot be the same as a column in REPORTING.ENRICHED_QUERY_HISTORY."
        except snowflake.snowpark.exceptions.SnowparkSQLException as e:
            if "invalid identifier" in e.message:
                pass
            else:
                assert False, "Invalid label name."

        return values

    @validator("group_rank", allow_reuse=True)
    def group_rank_is_numeric(cls, rank: Optional[int]) -> Optional[int]:
        # Pandas will give NaN instead of None which is a float
        if rank is None or math.isnan(rank):
            return None
        assert isinstance(rank, int), "Group rank must be an integer."
        return rank

    @validator("condition", allow_reuse=True)
    def condition_is_valid(cls, condition: str) -> str:
        assert condition is not None, "Condition must not be null."
        assert isinstance(condition, str), "Condition should be a string."
        if not condition:
            raise ValueError(
                "Labels must have a Condition (a SQL expression which evaluates to a boolean)."
            )
        return condition

    @validator("label_created_at", "label_modified_at", allow_reuse=True)
    def verify_time_fields(cls, time: datetime.datetime) -> datetime.datetime:
        # auto-unwrap a pandas Timestamp
        if isinstance(time, pd.Timestamp):
            return time.to_pydatetime()
        assert isinstance(
            time, datetime.datetime
        ), "Time fields must be a datetime.datetime object"
        return time

    @validator("enabled", "is_dynamic", allow_reuse=True)
    def enabled_or_dynamic(cls, b: bool) -> bool:
        assert isinstance(b, bool)
        return b


class PredefinedLabel(Label):
    @classmethod
    def validate_all(cls, session: snowflake.snowpark.Session):
        """
        Parses all labels in the predefined_labels to verify that all of the labels are valid.
        :param session:
        :return:
        """
        df = session.sql("select * from internal.predefined_labels").to_pandas()
        # Lowercase all of the column names
        df.rename(columns={col_name: col_name.lower() for col_name in df.axes[1]}, inplace=True)
        for row in df.to_dict(orient='records'):
            # Validate each predefined label
            _ = Label.parse_obj(row)
