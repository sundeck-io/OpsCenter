from snowflake.snowpark import Row
from contextlib import contextmanager
from pydantic import BaseModel, ValidationError, field_validator, FieldValidationInfo
from typing import Optional, ClassVar, get_args, get_origin, Union
import datetime

## TODO
# add validation stuff
# update and delete
# migrate
# copy predefined labels to labels
# predefined label table
# same again for probes
# hook up to stored procs for (CRUD and validation)
# can we extract the snowpark parts of this to make it easier to test?
class Label(BaseModel):
    table_name: ClassVar[str] = "LABELS"
    name: str
    group_name: Optional[str] = None
    group_rank: int
    label_created_at: datetime.datetime
    condition: str
    enabled: bool
    label_modified_at: datetime.datetime
    is_dynamic: bool

    def create(self, session):
        cols = dict()
        for field, info in self.model_fields.items():
            if isinstance(type, type(info.annotation)):
                info_type = handle_type(info.annotation)
            else:
                info_type = handle_union(
                    get_args(info.annotation), get_origin(info.annotation)
                )
            cols[field] = info_type
        cols_str = ", ".join([f"{k} {v}" for k, v in cols.items()])
        session.sql(
            f"CREATE TABLE IF NOT EXISTS internal.{self.table_name} ({cols_str})"
        ).collect()

    def write(self, session):
        df = session.create_dataframe([Row(**dict(self))])
        df.write.mode("append").save_as_table(self.table_name)

    @field_validator('name')
    @classmethod
    def name_is_unique_among_other_labels(cls, name: str, info: FieldValidationInfo) -> str:
        return name



_TYPES = {
    "LABEL": Label,
}


@contextmanager
def transaction(session):
    txn_open = False
    try:
        session.sql("BEGIN").collect()
        txn_open = True
        yield session
        session.sql("COMMIT").collect()
    except:
        if txn_open:
            session.sql("ROLLBACK").collect()
        raise


def create_entity(session, entity_type, entity, validation_proc):
    with transaction(session) as txn:
        try:
            t = _TYPES.get(entity_type)
            if not t:
                raise ValueError(f"Unknown entity type: {entity_type}")
            obj = t(**entity)
            obj.write(txn)
        except ValidationError as e:
            errs = []
            for e in e.errors():
                if e["type"] == "assertion_error" or e["type"] == "value_error":
                    errs.append(e.args[0])
            outcome = "\n".join(errs)
            return outcome


def handle_type(t):
    if t == str:
        return "STRING"
    elif t == int:
        return "NUMBER"
    elif t == datetime.datetime:
        return "TIMESTAMP"
    elif t == bool:
        return "BOOLEAN"
    else:
        raise ValueError(f"Unknown type: {t}")


def handle_union(args, origin):
    if origin == Union and len(args) == 2 and type(None) == args[1]:
        return f"{handle_type(args[0])} NULL"
    else:
        raise ValueError(f"Unknown union: {args} {origin}")
