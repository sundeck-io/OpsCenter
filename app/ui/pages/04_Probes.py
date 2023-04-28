import streamlit as st
import sthelp
import probes

sthelp.chrome("Probes")

st.sidebar.title("Sundeck OpsCenter")

probes.display()
