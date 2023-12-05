from pydantic import ValidationError
from .labels import Label
from .probes import Probe
from .errors import summarize_error
from .session import snowpark_session
from .wh_sched import WarehouseSchedules, WarehouseAlterStatements

# A "registry" of CRUD types and the implementation class
_TYPES = {
    "LABEL": Label,
    "PROBE": Probe,
    "QUERY_MONITOR": Probe,
    "WAREHOUSE_SCHEDULES": WarehouseSchedules,
    "WAREHOUSE_ALTER_STATEMENTS": WarehouseAlterStatements,
}


def create_table(session, entity_type):
    with snowpark_session(session) as txn:
        t = _TYPES.get(entity_type)
        if not t:
            raise ValueError(f"Unknown entity type: {entity_type}")
        t.create_table(txn)
        return None


def create_entity(session, entity_type, entity):
    with snowpark_session(session) as txn:
        try:
            t = _TYPES.get(entity_type)
            if not t:
                raise ValueError(f"Unknown entity type: {entity_type}")
            obj = t.parse_obj(entity)
            obj.write(txn)
            return None
        except ValidationError as e:
            return summarize_error(f"Failed to create {entity_type.lower()}", e)
        except Exception as ae:
            return f"Failed to create {entity_type.lower()}: {str(ae)}"


def update_entity(session, entity_type: str, old_name: str, new_obj: dict):
    with snowpark_session(session) as txn:
        try:
            t = _TYPES.get(entity_type)
            if not t:
                raise ValueError(f"Unknown entity type: {entity_type}")
            # TODO the existing way we update dynamic labels is very busted. Need to differentiate normal from dynamic labels
            # Assuming the old label is dynamic just because the new one is dynamic is wrong but what the code currently does.
            if not old_name:
                return "Name must not be null"
            # Special case for handling dynamic labels
            if new_obj.get("is_dynamic", False):
                obj = t.construct(group_name=old_name, is_dynamic=True)
            else:
                # Instantiate a new object with the old name
                obj = t.construct(name=old_name)
            new_obj = t.parse_obj(new_obj)
            obj.update(txn, new_obj)
            return None
        except ValidationError as ve:
            return summarize_error(f"Failed to update {entity_type.lower()}", ve)
        except Exception as ae:
            return f"Failed to update {entity_type.lower()}: {str(ae)}"


def delete_entity(session, entity_type: str, name: str):
    with snowpark_session(session) as txn:
        try:
            t = _TYPES.get(entity_type)
            if not t:
                raise ValueError(f"Unknown entity type: {entity_type}")
            if name is None:
                return "Name must not be null"
            obj = t.construct(name=name)
            obj.delete(txn)
            return None
        except Exception as ae:
            return f"Failed to delete {entity_type.lower()}: {str(ae)}"
