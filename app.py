import streamlit as st
from dotenv import load_dotenv
from src.ingest import render_ingest
from src.display import render_dashboard

load_dotenv()
st.set_page_config(page_title="US Aviation — Public Data Explorer", layout="wide")
st.title("✈️ US Aviation — Public Data Explorer")

tab_ingest, tab_dash = st.tabs(["Ingest", "Dashboard"])
with tab_ingest:
    render_ingest()
with tab_dash:
    render_dashboard()
