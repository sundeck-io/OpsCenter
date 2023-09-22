import datetime
import pydantic
import streamlit as st
from connection import Connection
import session as general_session
from session import Mode
import setup
import config
from crud.errors import error_to_markdown
from crud.probes import Probe as ModelProbe
from crud.session import snowpark_session


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
        _ = self.snowflake.call(
            "INTERNAL.REPORT_ACTION",
            "probes",
            "list",
        )
        st.title("Query Probes")
        st.markdown(
            """
        #### Alert or cancel suspect query patterns
        OpsCenter probes can monitor your active query workload and watch for query patterns to be made aware of.
        This includes both queries that should be cancelled or queries that should be alerted on.
        """
        )

        if not config.has_sundeck():
            setup.setup_block()

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
            st.button(
                "New", key="create", on_click=self.session.do_create, args=[dict()]
            )
            return

        cols = [1, 4, 1, 0.5, 1, 1]
        header = st.columns(cols)
        header[0].text("Name")
        header[1].text("Condition")
        header[2].text("Notify Author")
        header[3].text("Cancel")
        header[4].text("Notify Others")
        header[5].text("Actions")

        for i, row in enumerate(data):
            columns = st.columns(cols)
            write_if(columns[0], row["NAME"])
            columns[1].code(row["CONDITION"], language="sql")
            columns[2].checkbox(
                label=f"via {row['NOTIFY_WRITER_METHOD'].capitalize()}",
                label_visibility="visible" if row["NOTIFY_WRITER"] else "hidden",
                value=row["NOTIFY_WRITER"],
                disabled=True,
                key=f"notify_writer{i}",
            )

            columns[3].checkbox(
                label="",
                label_visibility="hidden",
                value=row["CANCEL"],
                disabled=True,
                key=f"cancel{i}",
            )

            with columns[4]:
                st.text(row["NOTIFY_OTHER"])
                if len(row["NOTIFY_OTHER"]) > 0:
                    st.write(f"via {row['NOTIFY_OTHER_METHOD'].capitalize()}")

            with columns[5]:
                buttons = st.columns(3)
                probe = {
                    "name": row["NAME"],
                    "condition": row["CONDITION"],
                    "cancel": row["CANCEL"],
                    "notify_writer": row["NOTIFY_WRITER"],
                    "notify_writer_method": row["NOTIFY_WRITER_METHOD"],
                    "notify_other": row["NOTIFY_OTHER"],
                    "notify_other_method": row["NOTIFY_OTHER_METHOD"],
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

        st.button("New", key="create", on_click=self.session.do_create, args=[dict()])

    def on_create_click(
        self,
        name,
        condition,
        notify_writer,
        notify_writer_method,
        notify_other,
        notify_other_method,
        cancel,
    ):
        with st.spinner("Creating new probe..."):
            _ = self.snowflake.call(
                "INTERNAL.REPORT_ACTION",
                "probes",
                "create",
            )
            try:
                with snowpark_session(self.snowflake) as txn:
                    obj = ModelProbe.parse_obj(
                        {
                            "name": name,
                            "condition": condition,
                            "notify_writer": notify_writer,
                            "notify_writer_method": notify_writer_method.upper(),
                            "notify_other": notify_other,
                            "notify_other_method": notify_other_method.upper(),
                            "cancel": cancel,
                            "probe_created_at": datetime.datetime.now(),
                            "probe_modified_at": datetime.datetime.now(),
                        }
                    )
                    outcome = obj.write(txn)
                    if outcome is None:
                        self.session.set_toast("New probe created.")
                        self.session.do_list()
                        return

            except pydantic.ValidationError as ve:
                outcome = error_to_markdown("Error validating Probe.", ve)
            except AssertionError as ae:
                outcome = f"Error validating Probe. \n\n{str(ae)}"

        self.status.error(outcome)

    def on_update_click(
        self,
        oldname,
        name,
        condition,
        notify_writer,
        notify_writer_method,
        notify_other,
        notify_other_method,
        cancel,
    ):
        with st.spinner("Updating probe..."):
            _ = self.snowflake.call(
                "INTERNAL.REPORT_ACTION",
                "probes",
                "update",
            )
            try:
                with snowpark_session(self.snowflake) as txn:
                    old_probe = ModelProbe.construct(name=oldname)
                    new_probe = ModelProbe.parse_obj(
                        {
                            "name": name,
                            "condition": condition,
                            "notify_writer": notify_writer,
                            "notify_writer_method": notify_writer_method.upper(),
                            "notify_other": notify_other,
                            "notify_other_method": notify_other_method.upper(),
                            "cancel": cancel,
                            # TODO Should not overwrite created_at
                            "probe_created_at": datetime.datetime.now(),
                            "probe_modified_at": datetime.datetime.now(),
                        }
                    )
                    outcome = old_probe.update(txn, new_probe)

                    if outcome is None:
                        self.session.set_toast("Probe updated.")
                        self.session.do_list()
                        return
            except pydantic.ValidationError as ve:
                outcome = error_to_markdown("Error validating Probe.", ve)
            except AssertionError as ae:
                outcome = f"Error validating Probe. \n\n{str(ae)}"

        self.status.error(outcome)

    def on_delete_click(self, name):
        with st.spinner("Deleting probe..."):
            _ = self.snowflake.call(
                "INTERNAL.REPORT_ACTION",
                "probes",
                "delete",
            )
            with snowpark_session(self.snowflake) as txn:
                del_probe = ModelProbe.construct(name=name)
                del_probe.delete(txn)

            self.session.set_toast("Probe deleted.")
            self.session.do_list()

    def create_probe(self):
        st.title("New Probe")

        name = st.text_input(key="NAME", label="Probe Name")
        condition = st.text_area(key="CONDITION", label="Condition")
        with st.expander("When Probe Matches:", expanded=True):
            notify_writer = st.checkbox(key="NOTIFY_WRITER", label="Notify the author")
            notify_writer_method = st.radio(
                key="NOTIFY_WRITER_METHOD",
                label="via",
                options=("Email", "Slack"),
                index=0,
            )
            cancel = st.checkbox(key="CANCEL", label="Cancel the query")
            notify_other = st.text_area(
                key="NOTIFY_OTHER", label="Notify others (comma delimited)"
            )
            notify_other_method = st.radio(
                key="NOTIFY_OTHER_METHOD",
                label="via",
                options=("Email", "Slack"),
                index=0,
            )
        st.button(
            "Create",
            on_click=self.on_create_click,
            args=[
                name,
                condition,
                notify_writer,
                notify_writer_method,
                notify_other,
                notify_other_method,
                cancel,
            ],
        )
        st.button("Cancel", on_click=self.session.do_list)

    def edit_probe(self, update: dict):
        st.title("Edit Probe")
        name = st.text_input(key="NAME", label="Probe Name", value=update["name"])
        condition = st.text_area(
            key="CONDITION", label="Condition", value=update["condition"]
        )
        with st.expander("When Probe Matches:", expanded=True):
            notify_writer = st.checkbox(
                key="NOTIFY_WRITER",
                label="Notify the author",
                value=update["notify_writer"],
            )
            notify_writer_method = st.radio(
                key="NOTIFY_WRITER_METHOD",
                label="via",
                options=("Email", "Slack"),
                index=1 if update["notify_writer_method"].lower() == "slack" else 0,
            )
            st.divider()
            cancel = st.checkbox(
                key="CANCEL", label="Cancel the query", value=update["cancel"]
            )
            st.divider()
            notify_other = st.text_input(
                key="NOTIFY_OTHER",
                label="Notify others (comma delimited)",
                value=update["notify_other"],
            )
            notify_other_method = st.radio(
                key="NOTIFY_OTHER_METHOD",
                label="via",
                options=("Email", "Slack"),
                index=1 if update["notify_other_method"].lower() == "slack" else 0,
            )

        st.button(
            "Update",
            on_click=self.on_update_click,
            args=[
                update["name"],
                name,
                condition,
                notify_writer,
                notify_writer_method,
                notify_other,
                notify_other_method,
                cancel,
            ],
        )
        st.button("Cancel", on_click=self.session.do_list)


def write_if(column, value):
    if value is not None:
        column.write(value)
