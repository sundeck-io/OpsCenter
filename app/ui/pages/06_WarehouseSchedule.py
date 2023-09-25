import streamlit as st
import sthelp
from modules import add_custom_modules

sthelp.chrome("Warehouse Schedule")

# Load custom OpsCenter python modules
if not add_custom_modules():
    st.warning("Unable to load OpsCenter modules.")


import warehouses  # noqa E402

warehouses.display()
