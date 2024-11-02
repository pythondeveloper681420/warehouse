#.venv\Scripts\activate.bat
#pip freeze > requirements.txt

import streamlit as st

st.set_page_config(page_title="Page Title", layout="wide")
   
hide_streamlit_style = """
<style>
.main {
    overflow: auto;
}
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
.stApp [data-testid="stToolbar"]{
display:none;
.reportview-container {
    margin-top: -2em;
}
.stDeployButton {display:none;}
#stDecoration {display:none;}    
</style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True) 
st.write("Hello world")