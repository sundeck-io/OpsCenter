from modules import add_custom_modules
import streamlit as st
import sthelp

sthelp.chrome("Probes")
st.sidebar.title("Sundeck OpsCenter")

# Load custom OpsCenter python modules
if not add_custom_modules():
    st.warning("Unable to load OpsCenter modules.")

import probes
probes.display()
