"""AML review dashboard (Part 5). Run from this folder:  streamlit run app.py"""
import streamlit as st

st.set_page_config(page_title="AML review dashboard", page_icon=":material/policy:", layout="wide")

pages = [
    st.Page("views/review_queue.py", title="Review queue", icon=":material/format_list_numbered:", default=True),
    st.Page("views/account_investigation.py", title="Account investigation", icon=":material/hub:"),
    st.Page("views/sharing_experiment.py", title="Sharing experiment", icon=":material/share:"),
]
st.navigation(pages).run()
