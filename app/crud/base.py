from contextlib import contextmanager
from snowflake.snowpark import Row
from pydantic import BaseModel
from typing import ClassVar, get_args, get_origin, Union, Dict, List
import datetime
import pandas as pd
from enum import Enum


## TODO
# same again for probes
# hook up to stored procs for (CRUD and validation)
class BaseOpsCenterModel(BaseModel):
    # The name of the table in snowflake (without schema) that the model maps to.
    table_name: ClassVar[str] = None

    @classmethod
    def cols_dict(cls) -> Dict[str, str]:
        cols = dict()
        for field, info in cls.__fields__.items():
            if isinstance(type, type(info.annotation)):
                info_type = handle_type(info.annotation)
            else:
                info_type = handle_union(
                    get_args(info.annotation), get_origin(info.annotation)
                )
            cols[field] = info_type
        return cols

    @classmethod
    def create_table(cls, session, with_catalog_view=True):
        cols = cls.cols_dict()
        cols_str = ", ".join([f"{k} {v}" for k, v in cols.items()])
        session.sql(
            f"CREATE TABLE IF NOT EXISTS internal.{cls.table_name} ({cols_str})"
        ).collect()
        if with_catalog_view:
            session.sql(
                f"CREATE OR REPLACE VIEW catalog.{cls.table_name} AS SELECT * FROM internal.{cls.table_name}"
            ).collect()

    def to_row(self) -> Row:
        return Row(**dict(self))

    def write(self, session):
        df = session.create_dataframe([self.to_row()])
        df.write.mode("append").save_as_table(
            f"INTERNAL.{self.table_name}", column_order="name"
        )

    def get_id(self) -> str:
        """
        Returns the column value for the current row which is unique among all other rows.
        :return:
        """
        return None

    def get_id_col(self) -> str:
        """
        Returns the name of the column which is unique among all other rows.
        :return:
        """
        return None

    def delete(self, session):
        session.sql(
            f"DELETE FROM INTERNAL.{self.table_name} WHERE {self.get_id_col()} = ?",
            params=(self.get_id(),),
        ).collect()

    def update(self, session, obj) -> "BaseOpsCenterModel":
        cols = dict(obj)
        # Build up the SET clause and bind param values
        set_elements = []
        params = []
        for k, v in cols.items():
            # Specific versions of snowpark-python appear to have an issue handling None bind parameters
            if v is None:
                set_elements.append(f"{k} = NULL")
            else:
                set_elements.append(f"{k} = ?")
                params.append(unwrap_value(v))
        set_clause = ", ".join(set_elements)
        params.append(self.get_id())
        stmt = f"UPDATE INTERNAL.{self.table_name} SET {set_clause} WHERE {self.get_id_col()} = ?"
        session.sql(
            stmt,
            params=params,
        ).collect()
        return obj

    @classmethod
    def batch_write(cls, session, data: List["BaseOpsCenterModel"], overwrite=False):
        """
        Writes a list of objects to the table. If overwrite is True, the table is truncated before writing.
        :param session:
        :param data:
        :param overwrite:
        :return:
        """
        df = session.create_dataframe([d.to_row() for d in data])
        # column_order only matters in append mode, but does not fail if it is specified in non-append mode
        df.write.mode("overwrite" if overwrite else "append").save_as_table(
            f"INTERNAL.{cls.table_name}",
            column_order="name",
        )

    @classmethod
    def batch_read(
        cls, session, sortby=None, filter: lambda df: bool = None
    ) -> List["BaseOpsCenterModel"]:
        """
        Reads all rows from the table and returns them as a list of objects.
        :param session:
        :return:
        """
        df = session.table(f"INTERNAL.{cls.table_name}").to_pandas()
        df.columns = [c.lower() for c in df.columns]
        if filter:
            df = df[filter(df)]
        if sortby:
            df.sort_values(by=[sortby], inplace=True)
        arr = [cls(**dict(row)) for row in df.to_dict("records")]
        return arr

    @classmethod
    def from_df(cls, df) -> List["BaseOpsCenterModel"]:
        """
        Reads all rows from the table and returns them as a list of objects.
        :param session:
        :return:
        """
        df.columns = [c.lower() for c in df.columns]
        arr = [cls(**dict(row)) for row in df.to_dict("records")]
        return arr


def unwrap_value(v):
    """
    Unwraps the Enum value if `v` is an Enum. Else, returns the original value.
    """
    if isinstance(v, Enum):
        return unwrap_value(v.value)
    elif isinstance(v, pd.Timestamp):
        # Python driver cannot handle pandas timestamps
        return unwrap_value(v.to_pydatetime())
    elif isinstance(v, datetime.datetime):
        # Python bindvars requires the concrete SQL timestamp datatype
        # TODO figure out a way to annotate the datetime on the model impls rather than assume TS_NTZ
        return "TIMESTAMP_NTZ", v
    return v


def handle_type(t):
    if t == str:
        return "STRING"
    elif t == int:
        return "NUMBER"
    elif t == float:
        return "NUMBER"
    elif t == datetime.datetime:
        return "TIMESTAMP"
    elif t == datetime.time:
        return "TIME"
    elif t == bool:
        return "BOOLEAN"
    elif issubclass(t, Enum):
        enum_values = list(t.__members__.values())
        if len(enum_values) == 0:
            raise ValueError(f"Enum {t} has no values")
        return handle_type(type(enum_values[0].value))
    else:
        raise ValueError(f"Unknown type: {t}")


def handle_union(args, origin):
    if origin == Union and len(args) == 2 and type(None) == args[1]:
        return f"{handle_type(args[0])} NULL"
    if (
        origin == Union
        and len(args) == 3
        and handle_type(args[0]) == handle_type(args[1])
        and type(None) == args[2]
    ):
        return f"{handle_type(args[0])} NULL"
    else:
        raise ValueError(f"Unknown union: {args} {origin}")


@contextmanager
def transaction(session):
    """
    Wraps any commands in a Snowflake transaction, automatically calling COMMIT on successful execution. If a
    Python exception is caught, ROLLBACK is automatically called. Beware calling any DDL statements while using
    this transaction as Snowflake will throw an error.
    """
    txn_started = False
    try:
        session.sql("BEGIN").collect()
        txn_started = True
        yield session
        session.sql("COMMIT").collect()
    except Exception as e:
        if txn_started:
            session.sql("ROLLBACK").collect()
        raise e
