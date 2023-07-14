import streamlit as st
from connection import Connection
import session as general_session
from session import Mode
from telemetry import page_view, action


class Label:
    def __init__(self):
        self.status = st.empty()
        self.session = general_session.labels()
        self.session.show_toast(self.status)

        self.snowflake = Connection.get()

    def list_labels(self):
        page_view('Labels')
        st.title("Query Labels")
        st.write(
            """
        #### Categorize queries based on custom conditions

        Query labels can be defined to classify queries. This can be used to identify query activity behavior segments and
        combine with OpsCenter query costing to better understand overall usage.
        """
        )

        with st.sidebar.container():
            st.write(
                """
            ##### Ungrouped Labels
            Ungrouped labels are defined as independent of each other. They can be applied to any query and used in
            combination.

            ##### Grouped Labels
            In addition to ungrouped labels, a collections of labels can be created in a group. In this case, the labels are
            mutually exclusive and ordered within that group. This means that only one label within a group can be applied
            to a query. When a group is created, it automatically also creates a implicit "Other" category within that group
            to identify any query not otherwise labeled.
            """
            )

        data = self.snowflake.sql(
            "select * from internal.LABELS order by group_name, group_rank"
        ).collect()

        if len(data) == 0:
            st.write(
                """No labels defined. Labels help you categorize incoming queries, similar to GMail labels.
                        Labels can be used to create and filter reports."""
            )
            st.button("New", key="create", on_click=self.session.do_create, args=[None])
            st.button("New (in group)", on_click=self.session.do_create, args=[""])
            return

        groups = self.snowflake.sql(
            """
            with groups as (
                select distinct case when group_name is null then 'Ungrouped' else group_name end as g
                from internal.labels
                union all
                select 'Ungrouped'
            )
            select distinct g
            from groups
            order by IFF(g = 'Ungrouped', 0, 1), g"""
        ).collect()

        items = list(map(lambda m: m[0], groups))

        tabs = st.tabs(items)

        for x, tab in enumerate(tabs):
            with tab:
                grouped = items[x] != "Ungrouped"
                cols = [1, 4, 0.5, 1]

                header = st.columns(cols)
                header[0].text("Name")
                header[1].text("Condition")
                if grouped:
                    header[2].text("Rank")
                header[3].text("Actions")

                for i, row in enumerate(data):

                    if grouped and row["GROUP_NAME"] != items[x]:
                        continue
                    if not grouped and row["GROUP_NAME"] is not None:
                        continue

                    columns = st.columns(cols)
                    write_if(columns[0], row["NAME"])
                    columns[1].code(row["CONDITION"], language="sql")
                    if grouped:
                        columns[2].text(row["GROUP_RANK"])
                    with columns[3]:
                        buttons = st.columns(3)
                        label = {
                            "name": row["NAME"],
                            "group_name": row["GROUP_NAME"],
                            "group_rank": row["GROUP_RANK"],
                            "condition": row["CONDITION"],
                        }
                        buttons[0].button(
                            "✏️",
                            key=f"edit{i}-{x}",
                            on_click=self.session.do_edit,
                            args=[label],
                        )
                        buttons[1].button(
                            "🗑️",
                            key=f"delete{i}-{x}",
                            on_click=self.on_delete_click,
                            args=[row["NAME"]],
                        )

                if items[x] == "Ungrouped":
                    st.button(
                        "New",
                        key=f"create{x}",
                        on_click=self.session.do_create,
                        args=[None],
                    )
                    st.button(
                        "New (in group)",
                        key=f"create_group{x}",
                        on_click=self.session.do_create,
                        args=[""],
                    )
                else:
                    st.button(
                        "Add label to group",
                        key=f"create{x}",
                        on_click=self.session.do_create,
                        args=[items[x]],
                    )

    def on_create_click(self, name, group, rank, condition):
        action("Create Label")
        with st.spinner("Creating new label..."):
            outcome = self.snowflake.call(
                "ADMIN.CREATE_LABEL", name, group, rank, condition
            )

            if outcome is None:
                self.session.set_toast("New label created.")
                self.session.do_list()
                return

        self.status.error(outcome)

    def on_update_click(self, oldname, name, group, rank, condition):
        action("Update Label")
        outcome = None
        with st.spinner("Updating label..."):
            outcome = self.snowflake.call(
                "ADMIN.UPDATE_LABEL", oldname, name, group, rank, condition
            )

            if outcome is None:
                self.session.set_toast("Label updated.")
                self.session.do_list()
            else:
                self.status.error(outcome)

    def on_delete_click(self, name):
        action("Delete Label")
        with st.spinner("Deleting label..."):
            self.snowflake.call("ADMIN.DELETE_LABEL", name)
            self.session.set_toast("Label deleted.")
            self.session.do_list()

    def create_label(self, grouped: str):
        st.title("New Label")
        group = None
        rank = None

        if grouped is not None:
            group = st.text_input(
                key="GROUP_NAME",
                label="Group Name",
                value=grouped,
                disabled=(grouped != ""),
            )

        name = st.text_input(key="NAME", label="Label Name")
        condition = st.text_area(key="CONDITION", label="Condition")
        if grouped is not None:
            rank = st.number_input(
                key="GROUP_RANK", label="Rank", format="%i", value=10
            )
        st.button(
            "Create",
            on_click=lambda: self.on_create_click(name, group, rank, condition),
        )
        st.button("Cancel", on_click=self.session.do_list)

    def edit_label(self, update: dict):
        page_view("Edit Label")
        st.title("Edit Label")
        name = st.text_input(key="NAME", label="Label Name", value=update["name"])
        group = update["group_name"]
        rank = update["group_rank"]
        if group is not None:
            group = st.text_input(
                key="GROUP_NAME",
                label="Group Name",
                value=update["group_name"],
                disabled=True,
            )
            rank = st.number_input(
                key="GROUP_RANK",
                label="Group Rank",
                format="%i",
                value=update["group_rank"],
            )
        condition = st.text_area(
            key="CONDITION", label="Condition", value=update["condition"]
        )

        st.button(
            "Update",
            on_click=self.on_update_click,
            args=[update["name"], name, group, rank, condition],
        )
        st.button("Cancel", on_click=self.session.do_list)


def display():

    labels = Label()
    if labels.session.mode is Mode.LIST:
        labels.list_labels()
    elif labels.session.mode is Mode.CREATE:
        labels.create_label(labels.session.group_create)
    elif labels.session.mode is Mode.EDIT:
        labels.edit_label(labels.session.update)
    else:
        st.exception("Mode not detected.")
        st.text(labels.session)


def write_if(column, value):
    if value is not None:
        column.write(value)
