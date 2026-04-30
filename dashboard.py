import streamlit as st
import sqlite3
import pandas as pd
import os
import sys
from PIL import Image
 
# -------------------------------------------------------------------------
# PATH + DB
# -------------------------------------------------------------------------
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from compute_plus_minus import parse_starters_and_scores

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data/basketball.db")
 
def connect_db():
    return sqlite3.connect(DB_PATH)
 
# -------------------------------------------------------------------------
# PAGE CONFIG
# -------------------------------------------------------------------------
 
st.set_page_config(
    page_title="UWRF Men's Basketball Plus/Minus",
    layout="wide",
)
 
# -------------------------------------------------------------------------
# HEADER
# -------------------------------------------------------------------------
st.title("UWRF Men's Basketball Plus/Minus Dashboard")
st.caption("UW-River Falls Men's Basketball — Season Analytics")
st.divider()
 
# -------------------------------------------------------------------------
# LOAD DATA
# -------------------------------------------------------------------------
@st.cache_data
def load_games():
    conn = connect_db()
    games = pd.read_sql_query(
        "SELECT game_id, game_date, opponent, location FROM games ORDER BY game_date",
        conn
    )
    rows = []
    for _, row in games.iterrows():
        cursor = conn.cursor()
        cursor.execute(
            "SELECT line_text FROM raw_lines WHERE game_id = ? ORDER BY line_id",
            (row["game_id"],)
        )
        lines = [r[0] for r in cursor.fetchall()]
        _, uwrf, opp = parse_starters_and_scores(lines)
        result = ("W" if uwrf > opp else "L") if uwrf and opp else "-"
        score  = f"{uwrf} - {opp}" if uwrf and opp else "N/A"
        rows.append({
            "Date":     row["game_date"],
            "Opponent": row["opponent"],
            "Location": row["location"],
            "Score":    score,
            "Result":   result,
        })
    conn.close()
    return pd.DataFrame(rows)
 
 
@st.cache_data
def load_season_plus_minus():
    conn = connect_db()
    df = pd.read_sql_query(
        """
        SELECT player_name, games_played, plus_minus, min_played,
               plus_minus_per_40, stints, plus_minus_per_stint,
               points, points_per_40, assists, assists_per_40
        FROM season_plus_minus_stats
        ORDER BY plus_minus DESC
        """,
        conn
    )
    conn.close()
    return df
 
 
@st.cache_data
def load_season_box_stats():
    conn = connect_db()
    df = pd.read_sql_query(
        """
        SELECT player_name, games_played,
               fg_makes, fg_attempts, fg_percentage,
               pt3_makes, pt3_attempts, pt3_percentage,
               ft_makes, ft_attempts, ft_percentage,
               rebounds, fouls, turnovers, blocks, steals
        FROM player_season_stats
        ORDER BY player_name
        """,
        conn
    )
    conn.close()
    return df
 
 
@st.cache_data
def load_lineups():
    conn = connect_db()
    df = pd.read_sql_query(
        """
        SELECT l.lineup_id,
               l.player1, l.player2, l.player3, l.player4, l.player5,
               COUNT(DISTINCT ls.game_id) as games,
               SUM(CASE WHEN e.team = 'UWRF' THEN e.points ELSE 0 END) as pts_for,
               SUM(CASE WHEN e.team = 'Opponent' THEN e.points ELSE 0 END) as pts_against
        FROM lineups l
        JOIN lineup_states ls ON l.lineup_id = ls.lineup_id
        JOIN events e ON ls.game_id = e.game_id AND ls.event_num = e.event_num
        WHERE e.event_type IN ('made_2pt', 'made_3pt', 'made_ft')
        GROUP BY l.lineup_id
        ORDER BY (pts_for - pts_against) DESC
        """,
        conn
    )
    conn.close()
    df["Plus/Minus"] = df["pts_for"] - df["pts_against"]
    df["Lineup"] = df[["player1","player2","player3","player4","player5"]].apply(
        lambda r: ", ".join(r), axis=1
    )
    return df
 
 
# -------------------------------------------------------------------------
# TABS
# -------------------------------------------------------------------------
games_tab, players_tab, lineups_tab = st.tabs(["Games", "Players", "Lineups"])
 
 
# =========================================================================
# GAMES TAB
# =========================================================================
with games_tab:
    st.header("Game Log")
    games_df = load_games()
 
    def color_result(val):
        if val == "W": return "color: green; font-weight: bold"
        if val == "L": return "color: red; font-weight: bold"
        return ""
 
    st.dataframe(
        games_df.style.map(color_result, subset=["Result"]),
        use_container_width=True,
        hide_index=True,
    )
 
 
# =========================================================================
# PLAYERS TAB
# =========================================================================
with players_tab:
    st.header("Player Stats")
 
    subtab1, subtab2 = st.tabs(["Plus/Minus", "Box Score"])
 
    with subtab1:
        pm_df = load_season_plus_minus().rename(columns={
            "player_name": "Player", "games_played": "GP",
            "plus_minus": "+/-", "min_played": "MIN",
            "plus_minus_per_40": "+/-/40", "stints": "Stints",
            "plus_minus_per_stint": "+/-/Stint",
            "points": "PTS", "points_per_40": "PTS/40",
            "assists": "AST", "assists_per_40": "AST/40"
        })
 
        def color_pm(val):
            try:
                if float(val) > 0: return "color: green"
                if float(val) < 0: return "color: red"
            except: pass
            return ""
 
        st.dataframe(
            pm_df.style.map(color_pm, subset=["+/-", "+/-/40", "+/-/Stint"]),
            use_container_width=True,
            hide_index=True,
        )
 
    with subtab2:
        box_df = load_season_box_stats().rename(columns={
            "player_name": "Player", "games_played": "GP",
            "fg_makes": "FGM", "fg_attempts": "FGA", "fg_percentage": "FG%",
            "pt3_makes": "3PM", "pt3_attempts": "3PA", "pt3_percentage": "3P%",
            "ft_makes": "FTM", "ft_attempts": "FTA", "ft_percentage": "FT%",
            "rebounds": "REB", "fouls": "PF", "turnovers": "TO",
            "blocks": "BLK", "steals": "STL"
        })
        st.dataframe(box_df, use_container_width=True, hide_index=True)
 
 
# =========================================================================
# LINEUPS TAB
# =========================================================================
with lineups_tab:
    st.header("5-Man Lineups")
 
    lineups_df = load_lineups()
    display_lineups = lineups_df[["Lineup", "games", "pts_for", "pts_against", "Plus/Minus"]].rename(columns={
        "games": "Games", "pts_for": "Pts For", "pts_against": "Pts Against"
    })
 
    def color_lineup_pm(val):
        try:
            if float(val) > 0: return "color: green"
            if float(val) < 0: return "color: red"
        except: pass
        return ""
 
    st.dataframe(
        display_lineups.style.map(color_lineup_pm, subset=["Plus/Minus"]),
        use_container_width=True,
        hide_index=True,
    )
