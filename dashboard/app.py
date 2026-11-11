"""AML review dashboard (Part 5). Run from this folder:  streamlit run app.py"""
import hmac
import os
import time

import streamlit as st

st.set_page_config(page_title="AML review dashboard", page_icon=":material/policy:", layout="wide")

# Shared-password gate, on only when DASH_PASSWORD is set (the hosted dashboard); no page runs before it.
password = os.getenv("DASH_PASSWORD", "")
if password and not st.session_state.get("authenticated"):
    entered = st.text_input("Password", type="password")
    if entered:
        if hmac.compare_digest(entered.encode(), password.encode()):
            st.session_state["authenticated"] = True
            st.rerun()
        time.sleep(1)  # slows down guessing
        st.error("Wrong password.")
    st.stop()

pages = [
    st.Page("views/review_queue.py", title="Review queue", icon=":material/format_list_numbered:", default=True),
    st.Page("views/account_investigation.py", title="Account investigation", icon=":material/hub:"),
    st.Page("views/sharing_experiment.py", title="Sharing experiment", icon=":material/share:"),
]
st.navigation(pages).run()
