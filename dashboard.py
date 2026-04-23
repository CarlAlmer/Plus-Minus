import streamlit as st
from PIL import Image

# -------------------------------------------------------------------------
# link to dashboard: https://uwrfpluseminus.streamlit.app/
# -------------------------------------------------------------------------
 
# -------------------------------------------------------------------------
# PAGE CONFIG
# -------------------------------------------------------------------------
st.set_page_config(
    page_title="UWRF Basketball Plus/Minus",
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
games_tab, players_tab, lineups_tab, player_stats_tab = st.tabs(["Games", "Players", "Lineups", "Player Stats"])
 
with games_tab:
    st.header("Games")
    st.info("Game log and scores will go here.")
 
with players_tab:
    st.header("Players")
    st.info("Player plus/minus stats will go here.")
 
with lineups_tab:
    st.header("Lineups")
    st.info("5-man lineup tracking will go here.")

with player_stats_tab:
 st.header("Player Stats")
 st.info("Player Stats will go here.")
