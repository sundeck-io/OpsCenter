import streamlit as st
import config
import sthelp
from modules import add_custom_modules

if not add_custom_modules():
    st.warning("Unable to load OpsCenter modules.")
import permissions

sthelp.chrome()
permissions.setup_permissions()

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
    st.markdown(
        """
            Run the following command in snowsight to set up your Sundeck account and get started:
            ```
            begin
                var setup_script varchar;
                call sundeck_opscenter.admin.register() into :setup_script;
                execute immediate :setup_script;
            end;
            ```
            """
    )
