import streamlit as st
import sqlite3
import pandas as pd
import re
import os
import sys
 
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
import PIL.Image as PILImage
_logo_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Uw_river_falls_falcons_logo.png")
_logo = PILImage.open(_logo_path) if os.path.exists(_logo_path) else None
 
st.set_page_config(
    page_title="UWRF Men's Basketball",
    page_icon=_logo if _logo else "🏀",
    layout="wide",
)
 
# Show logo in top-left corner
if _logo:
    st.logo(_logo_path)
 
# -------------------------------------------------------------------------
# CSS
# -------------------------------------------------------------------------
st.markdown("""
<style>
.uwrf-banner {
    background: linear-gradient(135deg, #7A0A1C 0%, #C8102E 60%, #7A0A1C 100%);
    padding: 1.2rem 2rem;
    border-radius: 8px;
    margin-bottom: 1.5rem;
}
.uwrf-banner h1 { margin: 0; font-size: 1.8rem; font-weight: 800; color: #FFFFFF; }
.uwrf-banner p  { margin: 0.2rem 0 0; font-size: 0.9rem; color: rgba(255,255,255,0.85); }
div[data-testid="metric-container"] {
    border: 2px solid #C8102E;
    border-radius: 8px;
    padding: 0.75rem 1rem;
    background-color: #FBEEEE;
}
.stTabs [data-baseweb="tab-highlight"] { background-color: #C8102E !important; }
.stTabs [aria-selected="true"] { color: #C8102E !important; font-weight: 700; }
</style>
""", unsafe_allow_html=True)
 
# -------------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------------
def display_opponent(raw: str) -> str:
    return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", str(raw))
 
def humanize_player(name: str) -> str:
    if pd.isna(name) or not name:
        return "—"
    if "," in name:
        last, first = name.split(",", 1)
        return f"{first.strip().title()} {last.strip().title()}"
    return name.title()
 
def style_pm(val):
    try:
        v = float(val)
        if v > 0: return "color: green; font-weight: 600"
        if v < 0: return "color: #C8102E; font-weight: 600"
    except: pass
    return ""
 
def style_result(val):
    if val == "W": return "color: green; font-weight: 700"
    if val == "L": return "color: #C8102E; font-weight: 700"
    return ""
 
EVENT_LABELS = {
    "made_2pt": "Made 2PT",       "made_3pt": "Made 3PT",     "made_ft": "Made FT",
    "miss_2pt": "Missed 2PT",     "miss_3pt": "Missed 3PT",   "miss_ft": "Missed FT",
    "rebound_off": "Off Rebound", "rebound_def": "Def Rebound",
    "sub_in": "Sub In",           "sub_out": "Sub Out",
    "turnover": "Turnover",       "foul": "Foul",
    "assist": "Assist",           "steal": "Steal",
    "block": "Block",             "timeout": "Timeout",
    "jump_ball": "Jump Ball",     "deadball_rebound": "Dead Ball Rebound",
}
 
# -------------------------------------------------------------------------
# DATA LOADERS
# -------------------------------------------------------------------------
@st.cache_data
def load_all_games():
    conn = connect_db()
    games = pd.read_sql_query(
        "SELECT game_id, game_date, opponent, location, conference FROM games ORDER BY game_date",
        conn
    )
    rows = []
    for _, row in games.iterrows():
        c = conn.cursor()
        c.execute("SELECT line_text FROM raw_lines WHERE game_id=? ORDER BY line_id", (row["game_id"],))
        lines = [r[0] for r in c.fetchall()]
        _, uwrf, opp = parse_starters_and_scores(lines)
        result = ("W" if uwrf > opp else "L") if uwrf and opp else "-"
        margin = (uwrf - opp) if uwrf and opp else None
        rows.append({
            "game_id":    row["game_id"],
            "Date":       row["game_date"],
            "Opponent":   row["opponent"],
            "Location":   row["location"],
            "Conference": "Conference" if row["conference"] == 1 else "Non-Conference",
            "Score":      f"{uwrf} - {opp}" if uwrf and opp else "N/A",
            "Result":     result,
            "Margin":     margin,
            "uwrf_pts":   uwrf or 0,
            "opp_pts":    opp or 0,
        })
    conn.close()
    return pd.DataFrame(rows)
 
 
@st.cache_data
def load_player_game_stats(game_ids: tuple):
    if not game_ids:
        return pd.DataFrame()
    conn = connect_db()
    ph = ",".join("?" * len(game_ids))
    df = pd.read_sql_query(f"""
        SELECT pgs.game_id, g.game_date, g.opponent,
               pgs.player_name, pgs.plus_minus, pgs.min_played, pgs.stints,
               pgs.fg_makes, pgs.fg_attempts, pgs.fg_percentage,
               pgs.pt3_makes, pgs.pt3_attempts, pgs.pt3_percentage,
               pgs.ft_makes, pgs.ft_attempts, pgs.ft_percentage,
               pgs.points, pgs.assists, pgs.rebounds,
               pgs.orb, pgs.drb, pgs.steals, pgs.blocks,
               pgs.turnovers, pgs.fouls
        FROM player_game_stats pgs
        JOIN games g ON pgs.game_id = g.game_id
        WHERE pgs.game_id IN ({ph})
        ORDER BY g.game_date, pgs.player_name
    """, conn, params=list(game_ids))
    conn.close()
    return df
 
 
def _clock_to_seconds(period: int, clock_str: str) -> float:
    """Convert period + countdown clock to elapsed seconds from tip-off."""
    try:
        mins, secs = clock_str.split(":")
        remaining = int(mins) * 60 + int(secs)
    except Exception:
        return 0.0
    if period == 1:
        return (20 * 60) - remaining
    elif period == 2:
        return (20 * 60) + ((20 * 60) - remaining)
    else:
        return (40 * 60) + (period - 2 - 1) * (5 * 60) + ((5 * 60) - remaining)
 
 
def _compute_lineup_minutes(conn, game_ids) -> dict:
    """Return {lineup_id: total_minutes} across the given games."""
    ph = ",".join("?" * len(game_ids))
    cursor = conn.cursor()
 
    # Get max period per game to know where the game ends
    cursor.execute(
        f"SELECT game_id, MAX(period) FROM events WHERE game_id IN ({ph}) GROUP BY game_id",
        list(game_ids)
    )
    max_period = {r[0]: r[1] for r in cursor.fetchall()}
 
    cursor.execute(f"""
        SELECT ls.game_id, ls.lineup_id, ls.event_num, e.period, e.clock
        FROM lineup_states ls
        JOIN events e ON ls.game_id = e.game_id AND ls.event_num = e.event_num
        WHERE ls.game_id IN ({ph}) AND ls.lineup_id IS NOT NULL
        ORDER BY ls.game_id, ls.event_num
    """, list(game_ids))
    rows = cursor.fetchall()
 
    lineup_secs: dict = {}
    prev_game = None
    prev_lid  = None
    prev_elapsed = None
 
    for game_id, lineup_id, event_num, period, clock in rows:
        elapsed = _clock_to_seconds(period, clock)
 
        # New game — close previous game's last stint
        if game_id != prev_game and prev_lid is not None:
            mp = max_period.get(prev_game, 2)
            game_end = 40 * 60 if mp <= 2 else 40 * 60 + (mp - 2) * 5 * 60
            dur = game_end - prev_elapsed
            if dur > 0:
                lineup_secs[prev_lid] = lineup_secs.get(prev_lid, 0) + dur
 
        # Lineup change within same game
        if game_id == prev_game and lineup_id != prev_lid and prev_lid is not None:
            dur = elapsed - prev_elapsed
            if dur > 0:
                lineup_secs[prev_lid] = lineup_secs.get(prev_lid, 0) + dur
 
        if lineup_id != prev_lid or game_id != prev_game:
            prev_elapsed = elapsed
 
        prev_game = game_id
        prev_lid  = lineup_id
 
    # Close very last stint
    if prev_lid is not None:
        mp = max_period.get(prev_game, 2)
        game_end = 40 * 60 if mp <= 2 else 40 * 60 + (mp - 2) * 5 * 60
        dur = game_end - prev_elapsed
        if dur > 0:
            lineup_secs[prev_lid] = lineup_secs.get(prev_lid, 0) + dur
 
    return {lid: secs / 60.0 for lid, secs in lineup_secs.items()}
 
 
@st.cache_data
def load_lineups(game_ids: tuple):
    if not game_ids:
        return pd.DataFrame()
    conn = connect_db()
    ph = ",".join("?" * len(game_ids))
    df = pd.read_sql_query(f"""
        SELECT l.lineup_id, l.player1, l.player2, l.player3, l.player4, l.player5,
               COUNT(DISTINCT ls.game_id) as games,
               SUM(CASE WHEN e.team='UWRF'     THEN e.points ELSE 0 END) as pts_for,
               SUM(CASE WHEN e.team='Opponent' THEN e.points ELSE 0 END) as pts_against
        FROM lineups l
        JOIN lineup_states ls ON l.lineup_id = ls.lineup_id
        JOIN events e ON ls.game_id = e.game_id AND ls.event_num = e.event_num
        WHERE e.event_type IN ('made_2pt','made_3pt','made_ft')
          AND ls.game_id IN ({ph})
        GROUP BY l.lineup_id
        ORDER BY (pts_for - pts_against) DESC
    """, conn, params=list(game_ids))
 
    # Compute minutes played per lineup
    lineup_mins = _compute_lineup_minutes(conn, game_ids)
    conn.close()
 
    df["plus_minus"] = df["pts_for"] - df["pts_against"]
    df["minutes"]    = df["lineup_id"].map(lineup_mins).fillna(0).round(2)
    df["pm_per_40"]  = (df["plus_minus"] / df["minutes"].replace(0, float("nan")) * 40).round(2)
    df["Lineup"] = df[["player1","player2","player3","player4","player5"]].apply(
        lambda r: ", ".join(humanize_player(p) for p in r), axis=1
    )
    return df
 
 
@st.cache_data
def load_events(game_id: int):
    conn = connect_db()
    df = pd.read_sql_query("""
        SELECT e.event_num, e.period, e.clock, e.team,
               e.player, e.event_type, e.points, e.description, g.opponent
        FROM events e
        JOIN games g ON e.game_id = g.game_id
        WHERE e.game_id = ?
        ORDER BY e.event_num
    """, conn, params=[game_id])
    conn.close()
    return df
 
 
def aggregate_player_stats(pgs: pd.DataFrame) -> pd.DataFrame:
    if pgs.empty:
        return pd.DataFrame()
    agg = pgs.groupby("player_name").agg(
        games_played = ("game_id",      "nunique"),
        plus_minus   = ("plus_minus",   "sum"),
        min_played   = ("min_played",   "sum"),
        stints       = ("stints",       "sum"),
        points       = ("points",       "sum"),
        assists      = ("assists",      "sum"),
        rebounds     = ("rebounds",     "sum"),
        fg_makes     = ("fg_makes",     "sum"),
        fg_attempts  = ("fg_attempts",  "sum"),
        pt3_makes    = ("pt3_makes",    "sum"),
        pt3_attempts = ("pt3_attempts", "sum"),
        ft_makes     = ("ft_makes",     "sum"),
        ft_attempts  = ("ft_attempts",  "sum"),
        steals       = ("steals",       "sum"),
        blocks       = ("blocks",       "sum"),
        turnovers    = ("turnovers",    "sum"),
        fouls        = ("fouls",        "sum"),
    ).reset_index()
 
    def sd(n, d):
        return n.astype(float) / d.astype(float).replace(0, float("nan"))
 
    agg["fg_pct"]       = sd(agg["fg_makes"],  agg["fg_attempts"]).round(2)
    agg["fg3_pct"]      = sd(agg["pt3_makes"], agg["pt3_attempts"]).round(2)
    agg["ft_pct"]       = sd(agg["ft_makes"],  agg["ft_attempts"]).round(2)
    agg["pm_per_40"]    = (sd(agg["plus_minus"], agg["min_played"]) * 40).round(2)
    agg["pts_per_40"]   = (sd(agg["points"],     agg["min_played"]) * 40).round(2)
    agg["ast_per_40"]   = (sd(agg["assists"],    agg["min_played"]) * 40).round(2)
    agg["pm_per_stint"] = sd(agg["plus_minus"],  agg["stints"]).fillna(0).round(2)
    agg["Player"]       = agg["player_name"].apply(humanize_player)
    return agg
 
 
def show_player_pm_table(agg: pd.DataFrame):
    pm = agg[["Player","games_played","plus_minus","min_played","stints",
               "pm_per_40","pm_per_stint","points","pts_per_40",
               "assists","ast_per_40"]].copy()
    pm = pm.sort_values("plus_minus", ascending=False)
    pm.columns = ["Player","Games","Plus/Minus","Minutes","Stints",
                  "Plus/Minus per 40","Plus/Minus per Stint",
                  "Points","Points per 40","Assists","Assists per 40"]
    st.dataframe(
        pm.style
            .map(style_pm, subset=["Plus/Minus","Plus/Minus per 40","Plus/Minus per Stint"])
            .format({
                "Plus/Minus":           "{:+d}",
                "Minutes":              "{:.2f}",
                "Plus/Minus per 40":    "{:+.2f}",
                "Plus/Minus per Stint": "{:+.2f}",
                "Points per 40":        "{:.2f}",
                "Assists per 40":       "{:.2f}",
            }, na_rep="—"),
        use_container_width=True, hide_index=True,
    )
 
 
def show_player_box_table(agg: pd.DataFrame):
    box = agg[["Player","games_played",
                "fg_makes","fg_attempts","fg_pct",
                "pt3_makes","pt3_attempts","fg3_pct",
                "ft_makes","ft_attempts","ft_pct",
                "rebounds","steals","blocks","turnovers","fouls"]].copy()
    box = box.sort_values("Player")
    box.columns = ["Player","Games",
                   "FG Made","FG Att","FG %",
                   "3PT Made","3PT Att","3PT %",
                   "FT Made","FT Att","FT %",
                   "Rebounds","Steals","Blocks","Turnovers","Fouls"]
    st.dataframe(
        box.style.format({
            "FG %": "{:.2f}", "3PT %": "{:.2f}", "FT %": "{:.2f}",
        }, na_rep="—"),
        use_container_width=True, hide_index=True,
    )
 
 
# -------------------------------------------------------------------------
# BANNER
# -------------------------------------------------------------------------
st.markdown("""
<div class="uwrf-banner">
    <h1>UWRF Men's Basketball</h1>
    <p>UW-River Falls — Plus/Minus Analytics Dashboard</p>
</div>
""", unsafe_allow_html=True)
 
# -------------------------------------------------------------------------
# LOAD BASE DATA
# -------------------------------------------------------------------------
all_games    = load_all_games()
all_game_ids = all_games["game_id"].tolist()
all_pgs      = load_player_game_stats(tuple(all_game_ids))
all_players  = sorted(all_pgs["player_name"].dropna().unique().tolist())
 
# -------------------------------------------------------------------------
# TABS — each tab has its own independent filters
# -------------------------------------------------------------------------
tab_games, tab_players, tab_lineups = st.tabs([
    "Games", "Players", "Lineups"
])
 
# =========================================================================
# GAMES TAB — filter and browse all games, click one to see details inline
# =========================================================================
with tab_games:
    st.header("Games")
 
    f1, f2, f3 = st.columns(3)
    with f1:
        g_loc  = st.radio("Site", ["All", "Home", "Away"], horizontal=True, key="g_loc")
    with f2:
        g_conf = st.radio("Conference", ["All", "Conference only", "Non-conference only"],
                          horizontal=True, key="g_conf")
    with f3:
        g_result = st.radio("Result", ["All", "Wins only", "Losses only"],
                            horizontal=True, key="g_result")
 
    gf = all_games.copy()
    if g_loc != "All":
        gf = gf[gf["Location"] == g_loc]
    if g_conf == "Conference only":
        gf = gf[gf["Conference"] == "Conference"]
    elif g_conf == "Non-conference only":
        gf = gf[gf["Conference"] == "Non-Conference"]
    if g_result == "Wins only":
        gf = gf[gf["Result"] == "W"]
    elif g_result == "Losses only":
        gf = gf[gf["Result"] == "L"]
 
    st.divider()
 
    wins  = (gf["Result"] == "W").sum()
    losses= (gf["Result"] == "L").sum()
    avg_m = gf["Margin"].mean()
 
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Games",      len(gf))
    m2.metric("Wins",       int(wins))
    m3.metric("Losses",     int(losses))
    m4.metric("Avg Margin", f"{avg_m:+.1f}" if pd.notna(avg_m) else "—")
 
    st.divider()
 
    disp = gf[["Date","Opponent","Location","Conference","Score","Result","Margin"]].copy()
    disp["Opponent"] = disp["Opponent"].apply(display_opponent)
    disp["Margin"]   = pd.to_numeric(disp["Margin"], errors="coerce")
 
    st.dataframe(
        disp.style
            .map(style_result, subset=["Result"])
            .map(style_pm,     subset=["Margin"])
            .format({"Margin": "{:+.0f}"}, na_rep="—"),
        use_container_width=True, hide_index=True,
    )
 
    st.divider()
    st.subheader("Game Detail")
    st.caption("Select a game to see player stats and play-by-play.")
 
    detail_options = gf["game_id"].tolist()
    if detail_options:
        sel_game = st.selectbox(
            "Choose a game",
            options=detail_options,
            format_func=lambda gid: (
                f"{all_games[all_games['game_id']==gid].iloc[0]['Date']}  "
                f"{display_opponent(all_games[all_games['game_id']==gid].iloc[0]['Opponent'])}  "
                f"({all_games[all_games['game_id']==gid].iloc[0]['Score']})"
            ),
            key="g_sel_game"
        )
 
        g2       = all_games[all_games["game_id"] == sel_game].iloc[0]
        opp_name = display_opponent(g2["Opponent"])
 
        st.subheader(f"{g2['Date']} — UWRF vs {opp_name}")
 
        dm1, dm2, dm3, dm4 = st.columns(4)
        dm1.metric("UWRF Pts",  g2["uwrf_pts"])
        dm2.metric("Opp Pts",   g2["opp_pts"])
        dm3.metric("Result",    g2["Result"])
        dm4.metric("Margin",    f"{g2['Margin']:+.0f}" if g2["Margin"] is not None else "—")
 
        st.divider()
 
        st.subheader("Player Stats")
        game_pgs = load_player_game_stats((sel_game,)).copy()
        if not game_pgs.empty:
            game_pgs["Player"] = game_pgs["player_name"].apply(humanize_player)
            game_pgs["pm_per_stint"] = (
                game_pgs["plus_minus"].astype(float) /
                game_pgs["stints"].astype(float).replace(0, float("nan"))
            ).fillna(0).round(2)
            gd = game_pgs[[
                "Player","plus_minus","min_played","stints","pm_per_stint",
                "points","assists","rebounds","steals","blocks","turnovers","fouls",
                "fg_makes","fg_attempts","fg_percentage",
                "pt3_makes","pt3_attempts","pt3_percentage",
                "ft_makes","ft_attempts","ft_percentage",
            ]].copy()
            gd.columns = [
                "Player","Plus/Minus","Minutes","Stints","Plus/Minus per Stint",
                "Points","Assists","Rebounds","Steals","Blocks","Turnovers","Fouls",
                "FG Made","FG Att","FG %","3PT Made","3PT Att","3PT %",
                "FT Made","FT Att","FT %",
            ]
            st.dataframe(
                gd.style
                    .map(style_pm, subset=["Plus/Minus","Plus/Minus per Stint"])
                    .format({
                        "Plus/Minus":           "{:+d}",
                        "Minutes":              "{:.2f}",
                        "Plus/Minus per Stint": "{:+.2f}",
                        "FG %": "{:.2f}", "3PT %": "{:.2f}", "FT %": "{:.2f}",
                    }, na_rep="—"),
                use_container_width=True, hide_index=True,
            )
 
        st.divider()
 
        st.subheader("Play by Play")
        ev = load_events(sel_game)
        if not ev.empty:
            ev = ev.copy()
            ev["Event"]  = ev["event_type"].map(EVENT_LABELS).fillna(ev["event_type"])
            ev["Player"] = ev["player"].apply(humanize_player)
            ev["Team"]   = ev["team"].apply(lambda t: "UWRF" if t == "UWRF" else opp_name)
            ev["Period"] = ev["period"].apply(lambda p: f"Half {p}" if p <= 2 else f"OT{p-2}")
            pbp = ev[["Period","clock","Team","Player","Event","description"]].copy()
            pbp.columns = ["Period","Clock","Team","Player","Event","Play"]
            st.dataframe(pbp, use_container_width=True, hide_index=True)
 
 
# =========================================================================
# PLAYERS TAB — independent filters, min minutes slider
# =========================================================================
with tab_players:
    st.header("Players")
 
    pf1, pf2, pf3 = st.columns(3)
    with pf1:
        p_players = st.multiselect(
            "Filter by player", options=all_players,
            format_func=humanize_player, key="p_players"
        )
    with pf2:
        p_games = st.multiselect(
            "Filter by game", options=all_game_ids,
            format_func=lambda gid: (
                f"{all_games[all_games['game_id']==gid].iloc[0]['Date']}  "
                f"{display_opponent(all_games[all_games['game_id']==gid].iloc[0]['Opponent'])}"
            ),
            key="p_games"
        )
    with pf3:
        p_conf = st.radio("Conference", ["All", "Conference only", "Non-conference only"],
                          horizontal=True, key="p_conf")
 
    pf4, pf5 = st.columns(2)
    with pf4:
        p_loc = st.radio("Site", ["All", "Home", "Away"], horizontal=True, key="p_loc")
    with pf5:
        p_result = st.radio("Result", ["All", "Wins only", "Losses only"], horizontal=True, key="p_result")
 
    p_min_minutes = st.slider(
        "Minimum total minutes played", min_value=0, max_value=400,
        value=0, step=1, key="p_min_min",
        help="Hide players who haven't played enough minutes for meaningful plus/minus"
    )
 
    # Build active game pool for players tab
    p_pool = all_games.copy()
    if p_conf == "Conference only":
        p_pool = p_pool[p_pool["Conference"] == "Conference"]
    elif p_conf == "Non-conference only":
        p_pool = p_pool[p_pool["Conference"] == "Non-Conference"]
    if p_loc != "All":
        p_pool = p_pool[p_pool["Location"] == p_loc]
    if p_result == "Wins only":
        p_pool = p_pool[p_pool["Result"] == "W"]
    elif p_result == "Losses only":
        p_pool = p_pool[p_pool["Result"] == "L"]
    p_ids = tuple(p_games) if p_games else tuple(p_pool["game_id"].tolist())
 
    pgs = load_player_game_stats(p_ids)
    if p_players:
        pgs = pgs[pgs["player_name"].isin(p_players)]
 
    st.divider()
 
    if pgs.empty:
        st.warning("No data for the selected filters.")
    else:
        agg = aggregate_player_stats(pgs)
        agg = agg[agg["min_played"] >= p_min_minutes]
 
        if agg.empty:
            st.warning(f"No players with at least {p_min_minutes} minutes played.")
        else:
            sub_pm, sub_box = st.tabs(["Plus/Minus", "Box Score"])
            with sub_pm:
                show_player_pm_table(agg)
            with sub_box:
                show_player_box_table(agg)
 
 
# =========================================================================
# LINEUPS TAB — independent filters, min minutes slider
# =========================================================================
with tab_lineups:
    st.header("5-Man Lineups")
 
    lf1, lf2 = st.columns(2)
    with lf1:
        l_games = st.multiselect(
            "Filter by game", options=all_game_ids,
            format_func=lambda gid: (
                f"{all_games[all_games['game_id']==gid].iloc[0]['Date']}  "
                f"{display_opponent(all_games[all_games['game_id']==gid].iloc[0]['Opponent'])}"
            ),
            key="l_games"
        )
        l_conf = st.radio("Conference", ["All", "Conference only", "Non-conference only"],
                          horizontal=True, key="l_conf")
    with lf2:
        l_players = st.multiselect(
            "Must include player", options=all_players,
            format_func=humanize_player, key="l_players"
        )
        l_loc = st.radio("Site", ["All", "Home", "Away"], horizontal=True, key="l_loc")
        l_result = st.radio("Result", ["All", "Wins only", "Losses only"], horizontal=True, key="l_result")
 
    l_min_minutes = st.slider(
        "Minimum minutes played by lineup", min_value=0, max_value=40,
        value=0, step=1, key="l_min_min",
        help="Hide lineups that haven't played enough minutes together"
    )
 
    l_pool = all_games.copy()
    if l_conf == "Conference only":
        l_pool = l_pool[l_pool["Conference"] == "Conference"]
    elif l_conf == "Non-conference only":
        l_pool = l_pool[l_pool["Conference"] == "Non-Conference"]
    if l_loc != "All":
        l_pool = l_pool[l_pool["Location"] == l_loc]
    if l_result == "Wins only":
        l_pool = l_pool[l_pool["Result"] == "W"]
    elif l_result == "Losses only":
        l_pool = l_pool[l_pool["Result"] == "L"]
    l_ids = tuple(l_games) if l_games else tuple(l_pool["game_id"].tolist())
 
    lin = load_lineups(l_ids)
 
    st.divider()
 
    if lin.empty:
        st.warning("No lineup data for the selected games.")
    else:
        if l_players:
            mask = lin[["player1","player2","player3","player4","player5"]].apply(
                lambda r: all(p in r.values for p in l_players), axis=1
            )
            lin = lin[mask]
 
        # Use games played as a proxy for minutes since we don't store lineup minutes
        # Filter by minimum games together instead
        if l_min_minutes > 0:
            lin = lin[lin["minutes"] >= l_min_minutes]
 
        if lin.empty:
            st.warning("No lineups match the selected filters.")
        else:
            disp_lin = lin[["Lineup","games","minutes","pts_for","pts_against","plus_minus","pm_per_40"]].copy()
            disp_lin.columns = ["Lineup","Games","Minutes","Pts For","Pts Against","Plus/Minus","Plus/Minus per 40"]
            st.dataframe(
                disp_lin.style
                    .map(style_pm, subset=["Plus/Minus","Plus/Minus per 40"])
                    .format({
                        "Plus/Minus":        "{:+d}",
                        "Minutes":           "{:.1f}",
                        "Plus/Minus per 40": "{:+.2f}",
                    }, na_rep="—"),
                use_container_width=True, hide_index=True,
            )
