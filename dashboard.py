import streamlit as st
from PIL import Image
 
# -------------------------------------------------------------------------
# PAGE CONFIG
# -------------------------------------------------------------------------
logo = Image.open("C:/Users/alber/OneDrive - University of Wisconsin-River Falls/" \
"Plus Minus Project/scripts/Uw_river_falls_falcons_logo.png")
st.set_page_config(
    page_title="UWRF Basketball Plus/Minus",
    page_icon=logo,
    layout="wide",
)
 
# -------------------------------------------------------------------------
# HEADER
# -------------------------------------------------------------------------
st.title("UWRF Basketball Plus/Minus Dashboard")
st.caption("UW-River Falls Men's Basketball — Season Analytics")
 
st.divider()
 
# -------------------------------------------------------------------------
# TABS
# -------------------------------------------------------------------------
games_tab, players_tab, lineups_tab = st.tabs(["Games", "Players", "Lineups"])
 
with games_tab:
    st.header("Games")
    st.info("Game log and scores will go here.")
 
with players_tab:
    st.header("Players")
    st.info("Player plus/minus stats will go here.")
 
with lineups_tab:
    st.header("Lineups")
    st.info("5-man lineup tracking will go here.")