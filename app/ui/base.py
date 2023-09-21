from session import Mode
from connection import Connection
import session as general_session
import streamlit as st
from typing import Optional
from crud.base import BaseOpsCenterModel


class Container:
    base_cls: Optional[BaseOpsCenterModel] = None
    ui_name: Optional[str] = None

    @classmethod
    def display(cls):
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
        pass

    def create(self, create):
        st.title(f"Create {self.ui_name}")
        out = self.create_internal(create)
        st.button(
            "Create",
            on_click=self.on_create_click,
            args=out,
        )
        st.button("Cancel", on_click=self.session.do_list)

    def create_internal(self, create):
        pass

    def edit(self, update):
        st.title("Edit {self.ui_name}")
        out = self.edit_internal(update)
        st.button(
            "Update",
            on_click=self.on_update_click,
            args=out,
        )
        st.button("Cancel", on_click=self.session.do_list)
        pass

    def edit_internal(self, update):
        pass

    def on_update_click_internal(self, *args) -> Optional[str]:
        pass

    def on_delete_click_internal(self, *args) -> Optional[str]:
        pass

    def on_create_click_internal(self, *args) -> Optional[str]:
        pass

    def on_create_click(self, *args):
        with st.spinner("Creating new f{self.ui_name.lower()}..."):
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
