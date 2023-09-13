from contextlib import contextmanager
from pydantic import ValidationError
import labels

## TODO
# test validation stuff
# migrate - maybe we don't move out of sql?
# copy predefined labels to labels - maybe we dont move out of sql?
# same again for probes
# hook up to stored procs for (CRUD and validation)
# more tests
_TYPES = {"LABEL": labels.Label, "PREDEFINED_LABEL": labels.PredefinedLabel}


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


def create_entity(session, entity_type, entity):
    with transaction(session) as txn:
        try:
            t = _TYPES.get(entity_type)
            if not t:
                raise ValueError(f"Unknown entity type: {entity_type}")
            obj = t.model_validate(entity, context={"session": txn})
            obj.write(txn)
        except ValidationError as e:
            errs = []
            for e in e.errors():
                if e["type"] == "assertion_error" or e["type"] == "value_error":
                    errs.append(e.args[0])
            outcome = "\n".join(errs)
            return outcome
