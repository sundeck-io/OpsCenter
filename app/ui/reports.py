import streamlit as st
import filters
import pandas as pd
import config


def display(options):
    if not config.get_materialization_complete():
        st.info("Please wait for materialization to complete before running reports.")
        st.button(
            "Refresh Status",
            on_click=config.refresh,
            key="refresh-materialization-status",
        )
        return

    credit_cost = config.get_compute_credit_cost()

    df = pd.DataFrame(list(options.keys()), columns=["tab"])
    report = st.selectbox("Select Report", df, index=0)

    st.title(report)
    filter_container = st.expander("Filters", expanded=False)
    st.container()

    filter_values = filters.display(filter_container)
    if filter_values.valid():

        with st.spinner(f"""Loading {report} Report"""):
            options[report](filter_values, credit_cost)
