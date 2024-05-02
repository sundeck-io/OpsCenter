import streamlit as st
import connection
import config
import sthelp
import pytz
from typing import Optional
from crud.tasks import Task
from crud.session import snowpark_session


sthelp.chrome("Settings")


def invalid_number(string: str) -> Optional[str]:
    try:
        n = float(string)  # Convert the string to float
        if n <= 0:
            return
        return string
    except ValueError:
        return


def get_task_status(dbname):
    sql = f"""describe task TASKS."{dbname}";"""
    df = connection.execute(sql)
    if df is None or len(df) == 0:
        return None
    return df["state"][0] == "started"


def get_task_state(status: bool):
    if status:
        return "RESUME"
    else:
        return "SUSPEND"


def task_listing(
    task_display_name: str,
    task_internal_name: str,
    frequency,
):
    try:
        status = get_task_status(task_internal_name)
    except Exception:
        return None, None

    return (
        st.checkbox(
            f"Enable {task_display_name} ({frequency})",
            value=status,
            key=f"enable_{task_internal_name}",
        ),
        status,
    )


tasks, config_tab, diagnostics_tab, reset, initial_probes = st.tabs(
    [
        "Tasks",
        "Config",
        "Diagnostics",
        "Reset",
        "Pre-Configured Probes/Labels",
    ]
)


with config_tab:
    form = st.form("Configuration")
    with form:
        compute_credit_cost, serverless_credit_cost, tbcost = config.get_costs()
        if compute_credit_cost is None:
            compute_credit_cost = 2.00
        if serverless_credit_cost is None:
            serverless_credit_cost = 3.00
        if tbcost is None:
            tbcost = 40.00
        timezone_start = config.Config.get("default_timezone")
        if timezone_start is None:
            timezone_start = "America/Los_Angeles"
        compute = st.text_input(
            "Compute Credit Cost", value=compute_credit_cost, key="compute_credit_cost"
        )
        serverless = st.text_input(
            "Serverless Credit Cost",
            value=serverless_credit_cost,
            key="serverless_credit_cost",
        )
        storage = st.text_input("Storage Cost (/tb)", value=tbcost, key="storage_cost")
        timezone = st.selectbox(
            "Default Timezone",
            index=next(
                i for i, v in enumerate(pytz.common_timezones) if v == timezone_start
            ),
            key="tz-select",
            options=pytz.common_timezones,
        )
        if st.form_submit_button("Save"):

            if (
                (clean_compute := invalid_number(compute)) is None
                or (clean_serverless := invalid_number(serverless)) is None
                or (clean_storage := invalid_number(storage)) is None
            ):
                st.error("Please enter a valid number for all costs.")

            elif timezone is None or timezone not in pytz.common_timezones:
                st.error("Please enter a valid timezone.")
            else:
                config.set_costs(clean_compute, clean_serverless, clean_storage)
                config.Config.set("default_timezone", timezone)
                connection.execute(
                    f"""
                BEGIN
                    CREATE OR REPLACE FUNCTION INTERNAL.GET_CREDIT_COST()
                        RETURNS NUMBER AS
                        $${compute}$$;

                    CREATE OR REPLACE FUNCTION INTERNAL.GET_SERVERLESS_CREDIT_COST()
                        RETURNS NUMBER AS
                        $${serverless}$$;

                    CREATE OR REPLACE FUNCTION INTERNAL.GET_STORAGE_COST()
                        RETURNS NUMBER AS
                        $${storage}$$;
                END;
                """
                )
                st.success("Saved")


def save_tasks(container, wem, qhm, pm, user_limits):
    with container:
        with st.spinner("Saving changes to task settings."):
            with connection.Connection.get() as conn:
                with snowpark_session(conn) as session:
                    for task_name, status in [
                        ("WAREHOUSE_EVENTS_MAINTENANCE", wem),
                        ("QUERY_HISTORY_MAINTENANCE", qhm),
                        ("SFUSER_MAINTENANCE", pm),
                        ("USER_LIMITS_MAINTENANCE", user_limits),
                    ]:
                        t = Task(task_name=task_name)
                        if status:
                            t.enable(session)
                        else:
                            t.disable(session)


with tasks:
    st.title("Tasks")

    checkboxes_container = st.empty()
    form = checkboxes_container.container()
    with form:
        wem, wems = task_listing(
            "Warehouse Events Maintenance",
            "WAREHOUSE_EVENTS_MAINTENANCE",
            "every hour",
        )
        qhm, qhms = task_listing(
            "Query History Maintenance",
            "QUERY_HISTORY_MAINTENANCE",
            "every hour",
        )
        pm, pms = task_listing(
            "Snowflake User Replication", "SFUSER_MAINTENANCE", "every day"
        )

        form_args = [form, wem, qhm, pm]

        # Only allow modification of user limits if Sundeck is linked
        user_limits_checkbox = None
        user_limits_enabled = False
        if config.has_sundeck():
            user_limits_checkbox, user_limits_enabled = task_listing(
                "User Limits Maintenance",
                "USER_LIMITS_MAINTENANCE",
                "every five minutes",
            )
        # user_limits_checkbox may be None at this point, handled in save_tasks
        form_args.append(user_limits_checkbox)

        # Only enable the button once the page has been reloaded and the checkbox is inconsistent with the task state. This is because streamlit
        # state is ugly and we don't want to record state here since it is already managed in Snowflake. Note this still has a bug if users
        # click multiple times quickly as the save button could be clicked before the last checkbox selection is recorded/refreshed.
        st.button(
            "Save Changes",
            on_click=save_tasks,
            args=form_args,
            disabled=(
                wems == wem
                and qhms == qhm
                and pms == pm
                and user_limits_checkbox == user_limits_enabled
            ),
        )

    if wem is None or qhm is None or pm is None:
        checkboxes_container.warning(
            "Unable to load task information. Make sure to run post-setup scripts."
        )

with diagnostics_tab:
    st.title("Diagnostics")

    st.markdown(
        """
        Diagnostics for OpsCenter relies on an [Snowflake Event Table](https://docs.snowflake.com/en/developer-guide/logging-tracing/event-table-setting-up)
        to store OpsCenter logging in your Snowflake Account as well as share OpsCenter errors with Sundeck to fix.

        Snowflake will automatically share logging and errors from OpsCenter with Sundeck after executing these steps.
        """
    )

    db = connection.execute_select("select current_database() as db").values[0][0]

    def expander(num: int, title: str) -> st.expander:
        return st.expander(f"Step {num}: {title}", expanded=True)

    # We can't inspect the account parameters to see if an event table is set because we're operating as the native
    # app and these commands cannot be run except as a caller. A human has to run these commands.
    with expander(1, "Create and Configure an Event Table"):
        st.markdown(
            """
            ### Event Table

            If you haven't already configured an event table for your account, follow these steps:

            These commands will create an event table and set it as the default event
            table for your account. Be sure to include use a database and schema that exists in your account.
            """
        )
        st.code(
            """
            -- Double check that there is no event table already set for your account before proceeding!
            SHOW PARAMETERS LIKE 'EVENT_TABLE' IN ACCOUNT;

            -- Create a database
            CREATE DATABASE my_database;

            -- Create the event table in that database
            CREATE EVENT TABLE my_database.public.my_events;

            -- Set this event table as the default for your account
            ALTER ACCOUNT SET EVENT_TABLE = my_database.public.my_events;
            """
        )
        st.markdown(
            """
            You can also follow the [Snowflake instructions](https://docs.snowflake.com/en/developer-guide/logging-tracing/event-table-setting-up) to
            set up an event table if you prefer.
            """
        )

    with expander(2, "Enable Diagnostic Sharing with Sundeck for OpsCenter"):
        st.markdown(
            """
            Sharing diagnostics with Sundeck helps us know when users are experiencing any errors in OpsCenter
            so we can fix them as soon as possible. To enable this, please run the following:
            """
        )
        st.code(
            f"""
            ALTER APPLICATION {db} SET SHARE_EVENTS_WITH_PROVIDER = true;
            """
        )


with reset:
    st.title("Reset/Reload")
    do_reset = st.button("Reset and reload query history and warehouse events.")
    if do_reset:
        bar = st.progress(0, text="Cleaning and refreshing query and warehouse events.")
        msg = st.empty()
        msg.warning("Resetting. Please do not navigate away from this page.")
        connection.execute(
            """
        begin
            truncate table internal.task_log;
            truncate table internal_reporting_mv.cluster_and_warehouse_sessions_complete_and_daily;
            truncate table internal_reporting_mv.query_history_complete_and_daily;
        end;
        """
        )
        bar.progress(
            10,
            text="Old activity removed, refreshing warehouse events. This may take a bit.",
        )
        connection.execute("call internal.refresh_warehouse_events(true);")
        bar.progress(
            30,
            text="Warehouse events refreshed, refreshing queries. This may take a bit.",
        )
        connection.execute("call internal.refresh_queries(true);")
        bar.progress(100, text="All events refreshed.")
        msg.info("Reset Complete.")

with initial_probes:
    st.title("Pre-Configured Probes and Labels")
    st.text(
        """
        OpsCenter includes a number of helpful labels and probes that are created when you have none defined.
        OpsCenter will not overwrite any existing labels and probes you have defined.
        Click the button below to reset all labels and probes to the defaults provided by OpsCenter.
        """
    )
    do_reset = st.button("Re-load all preconfigured labels and probes.")
    if do_reset:
        bar = st.progress(0, text="Loading preconfigured probes")
        msg = st.empty()
        msg.warning("Resetting. Please do not navigate away from this page.")
        connection.execute("call internal.merge_predefined_probes();")
        bar.progress(50, text="Loading preconfigured labels")
        connection.execute("call internal.merge_predefined_labels();")
        bar.progress(100, text="All events refreshed.")
        msg.info("Reset Complete.")
