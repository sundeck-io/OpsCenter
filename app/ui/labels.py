import datetime

import pydantic
import streamlit as st
from connection import Connection
import session as general_session
from session import Mode
from crud.labels import Label as ModelLabel
from crud.session import snowpark_session, operation
from crud.errors import error_to_markdown


class Label:
    def __init__(self):
        self.status = st.empty()
        self.session = general_session.labels()
        self.session.show_toast(self.status)

        self.snowflake = Connection.get()

    def list_labels(self):
        _ = self.snowflake.call(
            "INTERNAL.REPORT_ACTION",
            "labels",
            "list",
        )
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
            st.button(
                "New", key="create", on_click=self.session.do_create, args=[None, False]
            )
            st.button(
                "New (in group)", on_click=self.session.do_create, args=["", False]
            )
            st.button(
                "New dynamic grouped labels",
                on_click=self.session.do_create,
                args=["", True],
            )
            return

        groups = self.snowflake.sql(
            """
            with groups as (
                select distinct case when group_name is null then 'Ungrouped' else group_name end as g, is_dynamic
                from internal.labels
                union all
                select 'Ungrouped', FALSE
            )
            select distinct g, is_dynamic
            from groups
            order by IFF(g = 'Ungrouped', 0, 1), g"""
        ).collect()

        items = list(map(lambda m: m[0], groups))
        dynamics = list(map(lambda m: m[1], groups))

        tabs = st.tabs(items)

        for x, tab in enumerate(tabs):
            with tab:
                grouped = items[x] != "Ungrouped"
                cols = [1, 4, 0.5, 1]

                header = st.columns(cols)
                if dynamics[x] is False:
                    header[0].text("Name")
                else:
                    header[0].text("Dynamic grouped label")
                header[1].text("Condition")
                if grouped and dynamics[x] is False:
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
                    if grouped and dynamics[x] is False:
                        columns[2].text(row["GROUP_RANK"])
                    with columns[3]:
                        buttons = st.columns(3)
                        label = {
                            "name": row["NAME"],
                            "group_name": row["GROUP_NAME"],
                            "group_rank": row["GROUP_RANK"],
                            "condition": row["CONDITION"],
                            "is_dynamic": row["IS_DYNAMIC"],
                        }
                        buttons[0].button(
                            "✏️",
                            key=f"edit{i}-{x}",
                            on_click=self.session.do_edit,
                            args=[label],
                        )
                        if dynamics[x] is False:
                            buttons[1].button(
                                "🗑️",
                                key=f"delete{i}-{x}",
                                on_click=self.on_delete_click,
                                args=[row["NAME"], False],
                            )
                        else:
                            buttons[1].button(
                                "🗑️",
                                key=f"delete{i}-{x}",
                                on_click=self.on_delete_click,
                                args=[row["GROUP_NAME"], True],
                            )

                if items[x] == "Ungrouped":
                    st.button(
                        "New",
                        key=f"create{x}",
                        on_click=self.session.do_create,
                        args=[None, False],
                    )
                    st.button(
                        "New (in group)",
                        key=f"create_group{x}",
                        on_click=self.session.do_create,
                        args=["", False],
                    )
                    st.button(
                        "New dynamic grouped labels",
                        key=f"create_function_group{x}",
                        on_click=self.session.do_create,
                        args=["", True],
                    )
                elif dynamics[x] is False:
                    st.button(
                        "Add label to group",
                        key=f"create{x}",
                        on_click=self.session.do_create,
                        args=[items[x], False],
                    )

    def on_create_click(self, name, group, rank, condition, is_dynamic):
        with st.spinner("Creating new label..."):
            _ = self.snowflake.call(
                "INTERNAL.REPORT_ACTION",
                "labels",
                "create",
            )
            try:
                with snowpark_session(self.snowflake) as txn, operation('create') as op:
                    obj = ModelLabel.parse_obj(
                        {
                            "name": name,
                            "condition": condition,
                            "group_rank": rank,
                            "group_name": group,
                            "is_dynamic": is_dynamic,
                            "label_created_at": datetime.datetime.now(),
                            "label_modified_at": datetime.datetime.now(),
                        },
                    )
                    outcome = obj.write(txn)

                    if outcome is None:
                        self.session.set_toast("New label created.")
                        self.session.do_list()
                        return
            except pydantic.ValidationError as ve:
                outcome = error_to_markdown("Error validating Label.", ve)

        self.status.error(outcome)

    def on_update_click(self, oldname, name, group, rank, condition, is_dynamic):
        outcome = None
        with st.spinner("Updating label..."):
            _ = self.snowflake.call(
                "INTERNAL.REPORT_ACTION",
                "labels",
                "update",
            )

            try:
                with snowpark_session(self.snowflake) as sf, operation("update") as op:
                    # Make the old label, bypassing validation
                    old_label = ModelLabel.construct(name=oldname)
                    # Validate the new label before saving
                    new_label = ModelLabel.parse_obj(
                        {
                            'old_name': oldname,
                            "name": name,
                            "condition": condition,
                            "group_rank": rank,
                            "group_name": group,
                            "is_dynamic": is_dynamic,
                            "label_created_at": datetime.datetime.now(),
                            "label_modified_at": datetime.datetime.now(),
                        },
                    )
                    _ = old_label.update(sf, new_label)
                    outcome = None
            except pydantic.ValidationError as ve:
                outcome = error_to_markdown("Error updating Label.", ve)

            if outcome is None:
                self.session.set_toast("Label updated.")
                self.session.do_list()
            else:
                st.error(outcome)

    def on_delete_click(self, name, is_dynamic):
        with st.spinner("Deleting label..."):
            _ = self.snowflake.call(
                "INTERNAL.REPORT_ACTION",
                "labels",
                "delete",
            )
            with snowpark_session(self.snowflake) as txn, operation('delete') as op:
                # Make the old label, bypassing validation
                label_to_del = ModelLabel.construct(name=name)
                label_to_del.delete(txn)

            self.session.set_toast("Label deleted.")
            self.session.do_list()

    def create_label(self, grouped: str, is_dynamic: bool):
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
        name = None
        if not is_dynamic:
            name = st.text_input(key="NAME", label="Label Name")

        condition = st.text_area(key="CONDITION", label="Condition")
        if grouped is not None and not is_dynamic:
            rank = st.number_input(
                key="GROUP_RANK", label="Rank", format="%i", value=10
            )
        st.button(
            "Create",
            on_click=lambda: self.on_create_click(
                name, group, rank, condition, is_dynamic
            ),
        )
        st.button("Cancel", on_click=self.session.do_list)

    def edit_label(self, update: dict):
        st.title("Edit Label")
        is_dynamic = update["is_dynamic"]
        name = None
        if is_dynamic is False:
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
        if group is not None and is_dynamic is False:
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
            args=[update["name"], name, group, rank, condition, is_dynamic],
        )
        st.button("Cancel", on_click=self.session.do_list)


def display():

    labels = Label()
    if labels.session.mode is Mode.LIST:
        labels.list_labels()
    elif labels.session.mode is Mode.CREATE:
        labels.create_label(labels.session.group_create, labels.session.is_dynamic)
    elif labels.session.mode is Mode.EDIT:
        labels.edit_label(labels.session.update)
    else:
        st.exception("Mode not detected.")
        st.text(labels.session)


def write_if(column, value):
    if value is not None:
        column.write(value)
