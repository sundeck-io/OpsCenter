from snowflake.snowpark import Row
from contextlib import contextmanager


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


def create_entity(session, entity, table_name, validation_proc):
    with transaction(session) as txn:
        outcome = session.call(validation_proc, entity)
        if outcome:
            return outcome

        df = txn.create_dataframe([Row(**entity)])
        df.write.mode("append").save_as_table(table_name)
