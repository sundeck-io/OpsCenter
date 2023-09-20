# Make the entrypoints for SQL procedures available
import snowflake.snowpark

from .labels import PredefinedLabel
from .session import snowpark_session


def validate_predefined_labels(session: snowflake.snowpark.Session):
    with snowpark_session(session) as txn:
        PredefinedLabel.validate_all(txn)
