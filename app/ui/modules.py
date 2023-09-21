import os
import sys


def add_custom_modules() -> bool:
    """
    Adds all OpsCenter python modules to sys.path.
    :return:
    """
    if os.getenv("OPSCENTER_LOCAL_DEV"):
        return _load_crud_locally()

    return _add_zip_to_path(_get_crud_zip_file())


def _load_crud_locally() -> bool:
    """
    Alters sys.path based on the OpsCenter.git directory structure to avoid the
    need for a devdeploy every time a change is made to the CRUD python module.
    :return: True if the local CRUD python module was added to sys.path, False otherwise.
    """
    local_crud_path = os.path.join(os.path.dirname(__file__), "..")
    if not os.path.isdir(local_crud_path):
        print(f"Could not find CRUD python module at {local_crud_path}")
        return False

    if local_crud_path not in sys.path:
        print(f"Adding CRUD python module to path {local_crud_path}")
        sys.path.insert(0, local_crud_path)
    return True


def _get_crud_zip_file() -> str:
    """
    Returns the path to the crud.zip file.
    """
    return os.path.join(os.path.dirname(__file__), "crud.zip")


def _add_zip_to_path(zip_file: str) -> bool:
    """
    Adds a zip file to `sys.path` when it exists and is a file.
    :return: True if the given zipfile was added to sys.path, False otherwise.
    """
    if os.path.isfile(zip_file):
        sys.path.insert(0, zip_file)
        return True
    return False
