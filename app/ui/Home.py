import streamlit as st
import config
import sthelp
import setup

sthelp.chrome()

if not config.setup_complete():
    st.markdown(
        """
# Welcome To Sundeck OpsCenter

## Overview
OpsCenter is a community-driven, freely available Github project. It provides a set of tools to help you manage your
Snowflake account. If you want to know more about the capabilities, check them out
[here](https://sundeck.io/community/opscenter).

## Setup

Before using OpsCenter, you need to a few steps to get things set up. You can find the setup steps below.

"""
    )
    setup.setup_block()
else:
    st.markdown(
        """
    # Welcome To Sundeck OpsCenter

    You've successfully configured Sundeck OpsCenter. Check out the items on the sidebar.

    An overview dashboard will arrive here soon!
    """
    )
    if config.has_tenant_url():
        tenant_url = config.get_tenant_url()
        st.markdown(
            f"""
                [Go to my Sundeck account]({tenant_url})
            """
        )
