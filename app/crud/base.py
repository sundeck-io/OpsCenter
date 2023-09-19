from contextlib import contextmanager
from snowflake.snowpark import Row
from pydantic import BaseModel
from typing import ClassVar, get_args, get_origin, Union, Dict
import datetime


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

    def write(self, session):
        df = session.create_dataframe([Row(**dict(self))])
        df.write.mode("append").save_as_table(f"INTERNAL.{self.table_name}")

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
                params.append(v)
        set_clause = ", ".join(set_elements)
        params.append(self.get_id())
        session.sql(
            f"UPDATE INTERNAL.{self.table_name} SET {set_clause} WHERE {self.get_id_col()} = ?",
            params=params,
        ).collect()
        return obj


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
