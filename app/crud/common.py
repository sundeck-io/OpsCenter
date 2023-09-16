from contextlib import contextmanager
from pydantic import ValidationError
from .labels import Label, PredefinedLabel
from .session import session_ctx
from .errors import summarize_error

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
    #txn_open = False
    token = session_ctx.set(session)
    try:
        # TODO The call to internal.update_label_view fails with
        #  the error "Modifying a transaction that has started at a different scope is not allowed."
        #  when running inside of this block. Is the Snowpark dataframe doing something with txn?
        #session.sql("BEGIN").collect()
        #txn_open = True
        yield session
        #session.sql("COMMIT").collect()
    except:
        #if txn_open:
            #session.sql("ROLLBACK").collect()
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
            return ""
        except ValidationError as e:
            return summarize_error(f'Failed to create {entity_type}', e)
        except Exception as e:
            return str(e)


def update_entity(session, entity_type: str, old_name: str, new_obj: dict):
    with transaction(session) as txn:
        try:
            t = _TYPES.get(entity_type)
            if not t:
                raise ValueError(f"Unknown entity type: {entity_type}")
            obj = t.construct(name=old_name)
            new_label = Label.parse_obj(new_obj)
            obj.update(txn, new_label)
            return ""
        except ValidationError as e:
            return summarize_error(f'Failed to update {entity_type}', e)


def delete_entity(session, entity_type: str, name: str):
    with transaction(session) as txn:
        t = _TYPES.get(entity_type)
        if not t:
            raise ValueError(f"Unknown entity type: {entity_type}")
        obj = t.construct(name=name)
        obj.delete(txn)
        return ""
