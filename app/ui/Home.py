import streamlit as st
import config
import filters
import sthelp
import setup
import reports_heatmap

sthelp.chrome()
setup.setup_permissions()

st.markdown(
    """
# Welcome To Sundeck OpsCenter

## Overview
OpsCenter is a community-driven, freely available Github project. It provides a set of tools to help you manage your
Snowflake account. If you want to know more about the capabilities, check them out
[here](https://sundeck.io/community/opscenter).

Check out the items on the sidebar.
"""
)
if config.has_tenant_url():
    tenant_url = config.get_tenant_url()
    st.markdown(
        f"""
            To explore the Sundeck account, right-click on the link below and choose "Open link in new tab/window."

            [Go to my Sundeck account]({tenant_url})
        """
    )
else:
    if config.has_sundeck():
        st.markdown(
            """
                To explore the Sundeck account, right-click on the link below and choose "Open link in new tab/window."
                Then log in with your Sundeck credentials.

                [Go to my Sundeck account](https://sundeck.io/sign-in)
                """
        )

if not config.get_materialization_complete():
    st.info("Please wait for materialization to complete before running reports.")
    st.button(
        "Refresh Status",
        on_click=config.refresh,
        key="refresh-materialization-status",
    )
else:
    credit_cost = config.get_compute_credit_cost()

    st.markdown(
        """
## Warehouse Heatmap
This heatmap helps you understand how busy your Snowflake warehouses are. Ideally, warehouses should be 100%
utilized as anything less represents cost for active warehouses that are not running queries and generating values.
"""
    )

    filter_container = st.expander("Filters", expanded=False)
    st.container()

    filter_values = filters.display(filter_container)
    with st.spinner("Loading Warehouse Heatmap"):
        reports_heatmap.heatmap(filter_values, credit_cost)
