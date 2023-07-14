import streamlit as st
from connection import Connection
import session as general_session
from session import Mode
from telemetry import page_view, action


def display():
    probe = Probe()

    if probe.session.mode is Mode.LIST:
        probe.list_probes()
    elif probe.session.mode is Mode.CREATE:
        probe.create_probe()
    elif probe.session.mode is Mode.EDIT:
        probe.edit_probe(probe.session.update)
    else:
        st.exception("Mode not detected.")
        st.text(probe.session)


class Probe:
    def __init__(self):
        self.status = st.empty()
        self.session = general_session.probes()
        self.session.show_toast(self.status)

        self.snowflake = Connection.get()

    def list_probes(self):
        page_view('Probes')
        st.title("Query Probes")
        st.markdown(
            """
        #### Alert or cancel suspect query patterns
        OpsCenter probes can monitor your active query workload and watch for query patterns to be made aware of.
        This includes both queries that should be cancelled or queries that should be alerted on.
        """
        )

        with st.sidebar.container():
            st.markdown(
                """
            #### Probe Details
            ##### Execution Frequency
            Probes are executed by default every 1 minute.

            ##### Alert Behavior
            To enable email alerting behavior, you must first configure the email settings in the Settings page.

            """
            )
        data = self.snowflake.sql(
            "select * from internal.PROBES order by name"
        ).collect()

        if len(data) == 0:
            st.write(
                "No probes defined. Probes help you categorize alert and cancel running queries."
            )
            st.button("New", key="create", on_click=self.session.do_create, args=[None])
            return

        cols = [1, 4, 0.5, 0.5, 1, 1]
        header = st.columns(cols)
        header[0].text("Name")
        header[1].text("Condition")
        header[2].text("Email")
        header[3].text("Cancel")
        header[4].text("Email Others")
        header[5].text("Actions")

        for i, row in enumerate(data):
            columns = st.columns(cols)
            write_if(columns[0], row["NAME"])
            columns[1].code(row["CONDITION"], language="sql")
            columns[2].checkbox(
                label="email query submitter",
                label_visibility="hidden",
                value=row["EMAIL_WRITER"],
                disabled=True,
                key=f"email_writer{i}",
            )
            columns[3].checkbox(
                label="cancel query",
                label_visibility="hidden",
                value=row["CANCEL"],
                disabled=True,
                key=f"cancel{i}",
            )

            with columns[4]:
                st.text(row["EMAIL_OTHER"])

            with columns[5]:
                buttons = st.columns(3)
                probe = {
                    "name": row["NAME"],
                    "condition": row["CONDITION"],
                    "cancel": row["CANCEL"],
                    "email_writer": row["EMAIL_WRITER"],
                    "email_other": row["EMAIL_OTHER"],
                }
                buttons[0].button(
                    "✏️", key=f"edit{i}", on_click=self.session.do_edit, args=[probe]
                )
                buttons[1].button(
                    "🗑️",
                    key=f"delete{i}",
                    on_click=self.on_delete_click,
                    args=[row["NAME"]],
                )

        st.button("New", key="create", on_click=self.session.do_create, args=[None])

    def on_create_click(self, name, condition, email_writer, email_other, cancel):
        action("Create Probe")
        with st.spinner("Creating new probe..."):
            outcome = self.snowflake.call(
                "ADMIN.CREATE_PROBE",
                name,
                condition,
                email_writer,
                email_other,
                cancel,
            )

            if outcome is None:
                self.session.set_toast("New probe created.")
                self.session.do_list()
                return

        self.status.error(outcome)

    def on_update_click(
        self, oldname, name, condition, email_writer, email_other, cancel
    ):
        action("Update Probe")
        outcome = None
        with st.spinner("Updating probe..."):
            outcome = self.snowflake.call(
                "ADMIN.UPDATE_PROBE",
                oldname,
                name,
                condition,
                email_writer,
                email_other,
                cancel,
            )

            if outcome is None:
                self.session.set_toast("Probe updated.")
                self.session.do_list()
                return

        self.status.error(outcome)

    def on_delete_click(self, name):
        action("Delete Probe")
        with st.spinner("Deleting probe..."):
            self.snowflake.call("ADMIN.DELETE_PROBE", name)
            self.session.set_toast("Probe deleted.")
            self.session.do_list()

    def create_probe(self):
        st.title("New Probe")

        name = st.text_input(key="NAME", label="Probe Name")
        condition = st.text_area(key="CONDITION", label="Condition")
        with st.expander("When Probe Matches:", expanded=True):
            email_writer = st.checkbox(key="EMAIL_WRITER", label="Email the author")
            cancel = st.checkbox(key="CANCEL", label="Cancel the query")
            email_other = st.text_area(
                key="EMAIL_OTHER", label="Email others (comma delimited)"
            )
        st.button(
            "Create",
            on_click=self.on_create_click,
            args=[name, condition, email_writer, email_other, cancel],
        )
        st.button("Cancel", on_click=self.session.do_list)

    def edit_probe(self, update: dict):
        page_view("Edit Probe")
        st.title("Edit Probe")
        name = st.text_input(key="NAME", label="Probe Name", value=update["name"])
        condition = st.text_area(
            key="CONDITION", label="Condition", value=update["condition"]
        )
        with st.expander("When Probe Matches:", expanded=True):
            email_writer = st.checkbox(
                key="EMAIL_WRITER",
                label="Email the author",
                value=update["email_writer"],
            )
            cancel = st.checkbox(
                key="CANCEL", label="Cancel the query", value=update["cancel"]
            )
            email_other = st.text_input(
                key="EMAIL_OTHER",
                label="Email others (comma delimited)",
                value=update["email_other"],
            )

        st.button(
            "Update",
            on_click=self.on_update_click,
            args=[update["name"], name, condition, email_writer, email_other, cancel],
        )
        st.button("Cancel", on_click=self.session.do_list)


def write_if(column, value):
    if value is not None:
        column.write(value)
