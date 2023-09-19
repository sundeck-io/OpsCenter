import os
import sys


def add_custom_modules() -> bool:
    """
    Adds all modules bundled as zip files to sys.path.
    :return:
    """
    return add_zip_to_path(get_crud_zip_file())


def get_crud_zip_file() -> str:
    """
    Returns the path to the crud.zip file.
    """
    return os.path.join(os.path.dirname(__file__), "crud.zip")


def add_zip_to_path(zip_file: str) -> bool:
    """
    Adds a zip file to `sys.path` when it exists and is a file.
    :return: True if the given zipfile was added to sys.path, False otherwise.
    """
    if os.path.isfile(zip_file):
        sys.path.insert(0, zip_file)
        return True
    return False
