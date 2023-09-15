from contextlib import contextmanager
from pydantic import ValidationError
from .labels import Label, PredefinedLabel
from .session import session_ctx

## TODO
# test validation stuff
# migrate - maybe we don't move out of sql?
# copy predefined labels to labels - maybe we dont move out of sql?
# same again for probes
# hook up to stored procs for (CRUD and validation)
# more tests
_TYPES = {"LABEL": Label, "PREDEFINED_LABEL": PredefinedLabel}

@contextmanager
def transaction(session):
    txn_open = False
    token = session_ctx.set(session)
    try:
        session.sql("BEGIN").collect()
        txn_open = True
        yield session
        session.sql("COMMIT").collect()
    except:
        if txn_open:
            session.sql("ROLLBACK").collect()
        raise
    finally:
        session_ctx.reset(token)


def create_entity(session, entity_type, entity):
    with transaction(session) as txn:
        try:
            t = _TYPES.get(entity_type)
            if not t:
                raise ValueError(f"Unknown entity type: {entity_type}")
            obj = t.parse_obj(entity)
            obj.write(txn)
        except ValidationError as e:
            errs = []
            for e in e.errors():
                if e["type"] == "assertion_error" or e["type"] == "value_error":
                    errs.append(e.args[0])
            outcome = "\n".join(errs)
            return outcome


def update_entity(session, entity_type: str, old_name: str, new_obj: dict):
    with transaction(session) as txn:
        try:
            t = _TYPES.get(entity_type)
            if not t:
                raise ValueError(f"Unknown entity type: {entity_type}")
            obj = t(name=old_name)
            obj.update(txn, new_obj)
        except ValidationError as e:
            errs = []
            for e in e.errors():
                if e["type"] == "assertion_error" or e["type"] == "value_error":
                    errs.append(e.args[0])
            outcome = "\n".join(errs)
            return outcome
