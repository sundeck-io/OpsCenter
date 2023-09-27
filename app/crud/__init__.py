import snowflake.snowpark

# Make the entrypoints for SQL procedures available
from .common import (  # noqa F401
    create_entity,
    create_table,
    update_entity,
    delete_entity,
)
from .labels import PredefinedLabel
from .session import snowpark_session
from .wh_sched import WarehouseSchedulesTask, regenerate_alter_statements  # noqa F401


def validate_predefined_labels(sess: snowflake.snowpark.Session):
    with snowpark_session(sess) as txn:
        PredefinedLabel.validate_all(txn)
