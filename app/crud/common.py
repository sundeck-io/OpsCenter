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
        # Gotta fix this now, as we depend on rolling back the label insert if the view regeneration fails
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
            return None
        except ValidationError as e:
            return summarize_error(f'Failed to create {entity_type.lower()}', e)
        except Exception as ae:
            return f'Failed to create {entity_type.lower()}: {str(ae)}'


def update_entity(session, entity_type: str, old_name: str, new_obj: dict):
    with transaction(session) as txn:
        try:
            t = _TYPES.get(entity_type)
            if not t:
                raise ValueError(f"Unknown entity type: {entity_type}")
            # TODO the existing way we update dynamic labels is very busted. Need to differentiate normal from dynamic labels
            # Assuming the old label is dynamic just because the new one is dynamic is wrong but what the code currently does.
            if not old_name:
                return "Name must not be null"
            if new_obj.get("is_dynamic", False):
                obj = t.construct(group_name=old_name, is_dynamic=True)
            elif new_obj.get('group_name', None):
                obj = t.construct(group_name=old_name)
            else:
                obj = t.construct(name=old_name)
            new_label = Label.parse_obj(new_obj)
            obj.update(txn, new_label)
            return None
        except ValidationError as ve:
            return summarize_error(f'Failed to update {entity_type.lower()}', ve)
        except Exception as ae:
            return f'Failed to update {entity_type.lower()}: {str(ae)}'


def delete_entity(session, entity_type: str, name: str):
    with transaction(session) as txn:
        try:
            t = _TYPES.get(entity_type)
            if not t:
                raise ValueError(f"Unknown entity type: {entity_type}")
            if not name:
                return "Name must not be null"
            obj = t.construct(name=name)
            obj.delete(txn)
            return None
        except Exception as ae:
            return f'Failed to delete {entity_type.lower()}: {str(ae)}'
