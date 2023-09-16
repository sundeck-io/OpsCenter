import snowflake.snowpark.exceptions
from snowflake.snowpark import Row
from pydantic import (
    validator,
    root_validator,
)
from typing import Optional, ClassVar
import datetime
from .base import BaseOpsCenterModel
from .session import session_ctx

## TODO
# test validation stuff
# migrate - maybe we don't move out of sql?
# copy predefined labels to labels - maybe we dont move out of sql?
# same again for probes
# hook up to stored procs for (CRUD and validation)
# more tests
# ensure create, prepopulate, validate get called at the right times in bootstrap
class Label(BaseOpsCenterModel):
    table_name: ClassVar[str] = "LABELS"
    name: Optional[str] = None
    group_name: Optional[str] = None
    group_rank: Optional[int] = None
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
        super().delete(session)
        session.call("internal.update_label_view")

    def write(self, session):
        super().write(session)
        session.call("internal.update_label_view")

    def update(self, session, obj) -> "Label":
        if self.is_dynamic:
            old_label_exists = session.sql(
                f"SELECT COUNT(*) = 1 FROM INTERNAL.{self.table_name} WHERE group_name = '{self.group_name}' and is_dynamic"
            ).collect()[0][0]
        else:
            old_label_exists = session.sql(
                f"SELECT COUNT(*) = 1 FROM INTERNAL.{self.table_name} WHERE name = '{self.name}'"
            ).collect()[0][0]
            new_name_is_unique = session.sql(
                f"SELECT COUNT(*) = 0 FROM INTERNAL.{self.table_name} WHERE name = '{obj.name}' and name <> '{self.name}'"
            ).collect()[0][0]
            assert (
                new_name_is_unique
            ), "Label with this name already exists. Please choose a distinct name."

        assert (
            old_label_exists
        ), "Label not found. Please refresh your page to see latest list of labels."
        # handle updating timestamp(s)
        super().update(session, obj)
        session.call("internal.update_label_view")
        return obj

    @root_validator(allow_reuse=True)
    @classmethod
    def validate_label_obj(cls, values) -> "Label":
        """
        Validates that the attributes on this Label appear valid by inspecting only the Label itself.
        """
        name = values.get('name')
        if values.get('is_dynamic'):
            assert not name, "Dynamic labels cannot have a name"
            assert not values.get('group_rank'), "Dynamic labels cannot have a group rank"
            assert values.get('group_name', None) is not None, "Dynamic labels must have a group name"
        elif values.get('group_name'):
            group_name = values.get('group_name')
            assert group_name is not None, 'Name must not be null'
            assert isinstance(group_name, str), 'Label name should be a string'
            assert (
                values.get('group_rank')
            ), "Grouped labels must have a rank"
        else:
            assert name is not None, 'Name must not be null'
            assert isinstance(name, str), 'Label name must be a string'
            assert (
                    not values.get('group_rank')
                ), "Rank may only be provided for grouped labels"

        return values

    @root_validator(allow_reuse=True)
    @classmethod
    def validate_label_against_db(cls, values) -> "Label":
        """
        Validates this Label against the database to check things like name uniqueness and condition validity.
        """
        session = session_ctx.get('session')
        assert session, 'Session must be present'
        # Cannot check the condition if it's empty
        condition = values.get('condition')
        if condition:
            if values.get('is_dynamic'):
                stmt = f"select substring({condition}, 0, 0) from reporting.enriched_query_history where false"
            else:
                stmt = f"select case when {condition} then 1 else 0 end from reporting.enriched_query_history where false"
            try:
                session.sql(stmt).collect()
            except snowflake.snowpark.exceptions.SnowparkSQLException as e:
                assert False, f'Invalid label condition: "{e.message}"'

        # Check that the label [group] name does not conflict with any columns already in this view
        name = values.get('group_name') if values.get('group_name') else values.get('name')
        name_check = f'select "{name}" from reporting.enriched_query_history where false'

        try:
            session.sql(name_check).collect()
            assert False, 'Name cannot be the same as a column in REPORTING.ENRICHED_QUERY_HISTORY. Please use a different name.'
        except snowflake.snowpark.exceptions.SnowparkSQLException as e:
            if 'invalid identifier' in e.message:
                pass
            else:
                assert False, 'Invalid label name.'
        return values

    @validator("condition")
    def condition_is_valid(cls, condition: str) -> str:
        assert condition is not None, 'Condition must not be null'
        assert isinstance(condition, str), 'Label condition should be a string'
        if not condition:
            raise ValueError('Labels must have a Condition (a SQL expression which evaluates to a boolean).')
        return condition

    @validator("label_created_at", "label_modified_at")
    def verify_time_fields(
        cls, time: datetime.datetime
    ) -> datetime.datetime:
        assert isinstance(time, datetime.datetime)
        return time

    @validator("enabled", "is_dynamic")
    def enabled_or_dynamic(cls, b: bool) -> bool:
        assert isinstance(b, bool)
        return b


class PredefinedLabel(Label):
    table_name: ClassVar[str] = "PREDEFINED_LABELS"

    @classmethod
    def prepopulate(cls, session):
        labels = [
            ("Large Results", "rows_produced > 50000000"),
            ("Writes", "query_type in ('CREATE_TABLE_AS_SELECT', 'INSERT')"),
            ("Expanding Output", "10*bytes_scanned < BYTES_WRITTEN_TO_RESULT"),
            (
                "Full Scans",
                "coalesce(partitions_scanned, 0) > coalesce(partitions_total, 1) * 0.95",
            ),
            ("Long Compilation", "COMPILATION_TIME > 100"),
            ("Long Queries", "TOTAL_ELAPSED_TIME > 600000"),
            ("Expensive Queries", "COST>0.5"),
            ("Accelerated Queries", "QUERY_ACCELERATION_BYTES_SCANNED > 0"),
        ]
        if session.sql(
            "select system$behavior_change_bundle_status('2023_06') = 'ENABLED'"
        ).collect()[0][0]:
            labels.extend(
                [
                    (
                        "Repeated Queries",
                        "tools.is_repeated_query(query_parameterized_hash, 1000)",
                    ),
                    (
                        "ad-hoc Queries",
                        "tools.is_ad_hoc_query(query_parameterized_hash, 10)",
                    ),
                ]
            )
        rows = []
        for name, condition in labels:
            o = cls.model_validate(
                {
                    "name": name,
                    "condition": condition,
                    "enabled": True,
                    "is_dynamic": False,
                    "created_at": datetime.datetime.now(),
                    "modified_at": datetime.datetime.now(),
                },
                context={"session": session},
            )
            rows.append(Row(**dict(o)))
        df = session.create_dataframe(rows)
        df.write.mode("overwrite").save_as_table(cls.table_name)
