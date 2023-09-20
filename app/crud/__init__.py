import snowflake.snowpark

# Make the entrypoints for SQL procedures available
from .common import create_entity, update_entity, delete_entity # noqa F401
from .labels import PredefinedLabel
from .session import snowpark_session


def validate_predefined_labels(sess: snowflake.snowpark.Session):
    with snowpark_session(sess) as txn:
        PredefinedLabel.validate_all(txn)
