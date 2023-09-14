from snowflake.snowpark import Row
from pydantic import (
    field_validator,
    FieldValidationInfo,
    model_validator,
    ValidationInfo,
)
from typing import Optional, ClassVar
import datetime
from base import BaseOpsCenterModel

## TODO
# test validation stuff
# migrate - maybe we don't move out of sql?
# copy predefined labels to labels - maybe we dont move out of sql?
# same again for probes
# hook up to stored procs for (CRUD and validation)
# more tests
# ensure create, prepopulate, validate get called at the right times in bootstrap
# endure these files get put in a package in a stage and in the streamlit package
class Label(BaseOpsCenterModel):
    table_name: ClassVar[str] = "LABELS"
    name: Optional[str] = None
    group_name: Optional[str] = None
    group_rank: Optional[int] = None
    created_at: datetime.datetime  # todo should this have a default?
    condition: str
    enabled: bool
    modified_at: datetime.datetime  # todo should this have a default?
    is_dynamic: bool

    def get_id_col(self) -> str:
        return "name" if self.name else "group_name"

    def get_id(self) -> str:
        return self.name if self.name else self.group_name

    def delete(self, session):
        super().delete(session)
        session.call("internal.update_label_view();")

    def write(self, session):
        super().write(session)
        session.call("internal.update_label_view();")

    def update(self, session, obj) -> "Label":
        if self.is_dynamic:
            oldcnt = session.sql(
                f"SELECT COUNT(*) FROM {self.table_name} WHERE group_name = '{self.group_name}' and is_dynamic"
            ).collect()[0][0]
            newcnt = 0
        else:
            oldcnt = session.sql(
                f"SELECT COUNT(*) FROM {self.table_name} WHERE name = '{self.name}'"
            ).collect()[0][0]
            newcnt = session.sql(
                f"SELECT COUNT(*) FROM {self.table_name} WHERE name = '{obj.name}' and name <> '{self.name}"
            ).collect()[0][0]
        assert (
            newcnt == 0
        ), "Label with this name already exists. Please choose a distinct name."
        assert (
            oldcnt == 1
        ), "Label not found. Please refresh your page to see latest list of labels."
        # handle updating timestamp(s)
        super().update(session, obj)
        session.call("internal.update_label_view();")
        return obj

    @model_validator(mode="after")
    def validate_label_obj(self) -> "Label":
        """
        Validates that the attributes on this Label appear valid by inspecting only the Label itself.
        """
        if self.is_dynamic:
            assert not self.name, "Dynamic labels cannot have a name"
            assert not self.group_rank, "Dynamic labels cannot have a group rank"
            assert self.group_name, "Dynamic labels must have a group name"
        else:
            assert self.name, "Labels must have a name"
            if self.group_name:
                assert (
                    self.group_rank
                ), "Labels with a group name must have a group rank"
            else:
                assert (
                    not self.group_rank
                ), "Labels without a group name cannot have a group rank"
        return self

    @model_validator(mode="after")
    def validate_label_against_db(self, info: ValidationInfo) -> "Label":
        """
        Validates this Label against the database to check things like name uniqueness and condition validity.
        """
        ctx = info.context
        assert ctx, "Context must be present"
        print(ctx)
        assert ctx.get("session"), "Session must be present"
        session = ctx["session"]
        if self.is_dynamic:
            stmt = f"select substring({self.condition}, 0, 0) from reporting.enriched_query_history where false"
        else:
            stmt = f"select case when {self.condition} then 1 else 0 end from reporting.enriched_query_history where false"
        session.sql(stmt).collect()
        ## todo below should fail
        if self.group_name:
            session.sql(
                f'select "{self.group_name}" from reporting.enriched_query_history where false'
            ).collect()
        else:
            session.sql(
                f'select "{self.name}" from reporting.enriched_query_history where false'
            ).collect()

    @field_validator("name")
    @classmethod
    def name_is_a_string(
        cls, name: str, info: FieldValidationInfo
    ) -> str:
        assert isinstance(name, str)
        if not name:
            raise ValueError("Name cannot be empty")

        return name

    @field_validator("condition")
    @classmethod
    def condition_is_valid(cls, condition: str, info: FieldValidationInfo) -> str:
        assert isinstance(condition, str)
        if not condition:
            raise ValueError("Condition cannot be empty")
        return condition

    @field_validator("created_at", "modified_at")
    @classmethod
    def verify_time_fields(
        cls, time: datetime.datetime, info: FieldValidationInfo
    ) -> datetime.datetime:
        assert isinstance(time, datetime.datetime)
        return time

    @field_validator("enabled", "is_dynamic")
    @classmethod
    def enabled_or_dynamic(cls, b: bool, info: FieldValidationInfo) -> bool:
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
