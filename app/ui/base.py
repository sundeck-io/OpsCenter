from session import Mode
from connection import Connection
import session as general_session
import streamlit as st
from typing import Optional
from crud.base import BaseOpsCenterModel


class Container:
    """
    Base class for CRUD UI Elements
    """

    base_cls: Optional[BaseOpsCenterModel] = None
    ui_name: Optional[str] = None

    @classmethod
    def display(cls):
        """
        Connect edit modes to the underlying class and display the UI for the given mode
        """
        container = cls()

        if container.session.mode is Mode.LIST:
            container.list()
        elif container.session.mode is Mode.CREATE:
            container.create(container.session.create)
        elif container.session.mode is Mode.EDIT:
            container.edit(container.session.update)
        else:
            st.exception("Mode not detected.")
            st.text(container.session)

    def __init__(self):
        self.status = st.empty()
        self.session = general_session.Sessions.get(self.base_cls.table_name)
        self.session.show_toast(self.status)

        self.snowflake = Connection.get()

    def list(self):
        """
        Display the list of items
        """
        raise NotImplementedError("Implement this method in your subclass")

    def create(self, create):
        """
        Display the create form
        """
        st.title(f"Create {self.ui_name}")
        out = self.create_internal(create)
        st.button(
            "Create",
            on_click=self.on_create_click,
            args=out,
        )
        st.button("Cancel", on_click=self.session.do_list)

    def create_internal(self, create):
        """
        Implement this method in your subclass to display the individual widgets for editing subclass objects
        """
        raise NotImplementedError("Implement this method in your subclass")

    def edit(self, update):
        """
        Display the edit form
        """
        st.title(f"Edit {self.ui_name}")
        out = self.edit_internal(update)
        st.button(
            "Update",
            on_click=self.on_update_click,
            args=out,
        )
        st.button("Cancel", on_click=self.session.do_list)
        pass

    def edit_internal(self, update):
        """
        Implement this method in your subclass to display the individual widgets for editing subclass objects
        """
        raise NotImplementedError("Implement this method in your subclass")

    def on_update_click_internal(self, *args) -> Optional[str]:
        """
        Implement this method in your subclass to handle the update click
        """
        raise NotImplementedError("Implement this method in your subclass")

    def on_delete_click_internal(self, *args) -> Optional[str]:
        """
        Implement this method in your subclass to handle the delete click
        """
        raise NotImplementedError("Implement this method in your subclass")

    def on_create_click_internal(self, *args) -> Optional[str]:
        """
        Implement this method in your subclass to handle the create click
        """
        raise NotImplementedError("Implement this method in your subclass")

    def on_create_click(self, *args):
        """
        Handle the create click, show a spinner, call analytics. Show appropriate errors if failure.
        """
        with st.spinner(f"Creating new {self.ui_name.lower()}..."):
            _ = self.snowflake.call(
                "INTERNAL.REPORT_ACTION",
                f"{self.ui_name.lower()}s",
                "create",
            )
            outcome = self.on_create_click_internal(*args)
            if outcome is None:
                self.session.set_toast(f"New {self.ui_name.lower()} created.")
                self.session.do_list()
                return

        self.status.error(outcome)

    def on_delete_click(self, *args):
        """
        Handle the delete click, show a spinner, call analytics. Show appropriate errors if failure.
        """
        with st.spinner("Deleting label..."):
            _ = self.snowflake.call(
                "INTERNAL.REPORT_ACTION",
                f"{self.ui_name.lower()}s",
                "delete",
            )
            self.on_delete_click_internal(*args)
            self.session.set_toast(f"{self.ui_name} deleted.")
            self.session.do_list()

    def on_update_click(self, *args):
        """
        Handle the update click, show a spinner, call analytics. Show appropriate errors if failure.
        """
        with st.spinner(f"Updating {self.ui_name.lower()}..."):
            _ = self.snowflake.call(
                "INTERNAL.REPORT_ACTION",
                f"{self.ui_name.lower()}s",
                "update",
            )
            outcome = self.on_update_click_internal(*args)

            if outcome is None:
                self.session.set_toast(f"{self.ui_name} updated.")
                self.session.do_list()
                return

        self.status.error(outcome)
