import sqlite3
 
# this will create the database file if it doesn't exist
conn = sqlite3.connect("basketball.db")
 
cursor = conn.cursor()
 
# -------------------------
# games table
# -------------------------
cursor.execute("""
CREATE TABLE IF NOT EXISTS games (
    game_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_date TEXT,
    opponent TEXT,
    location TEXT,
    source_url TEXT
)
""")
 
# -------------------------
# raw play-by-play lines
# -------------------------
cursor.execute("""
CREATE TABLE IF NOT EXISTS raw_lines (
    line_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id INTEGER,
    line_text TEXT
)
""")
 
# -------------------------
# parsed events
# -------------------------
cursor.execute("""
CREATE TABLE IF NOT EXISTS events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id INTEGER,
    event_num INTEGER,
    period INTEGER,
    clock TEXT,
    team TEXT,
    player TEXT,
    event_type TEXT,
    points INTEGER,
    description TEXT
)
""")
 
# -------------------------
# distinct 5-man lineups
#
# Each row represents a unique combination of 5 players, stored sorted
# alphabetically so the same group always maps to the same lineup_id
# regardless of the order players subbed in at runtime.
# -------------------------
cursor.execute("""
CREATE TABLE IF NOT EXISTS lineups (
    lineup_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    player1     TEXT NOT NULL,
    player2     TEXT NOT NULL,
    player3     TEXT NOT NULL,
    player4     TEXT NOT NULL,
    player5     TEXT NOT NULL,
    UNIQUE (player1, player2, player3, player4, player5)
)
""")
 
# -------------------------
# lineup states
#
# One row per event, recording which players were on the floor.
# lineup_id references the lineups table when all 5 slots are filled;
# it is NULL whenever fewer than 5 players are on the floor for that event.
# -------------------------
cursor.execute("""
CREATE TABLE IF NOT EXISTS lineup_states (
    state_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id     INTEGER,
    event_num   INTEGER,
    player1     TEXT,
    player2     TEXT,
    player3     TEXT,
    player4     TEXT,
    player5     TEXT,
    lineup_id   INTEGER REFERENCES lineups (lineup_id)
)
""")
 
# -------------------------
# player stats per game
# -------------------------
cursor.execute("""
CREATE TABLE IF NOT EXISTS player_game_stats (
    stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id INTEGER,
    player_name TEXT,
    plus_minus INTEGER,
    min_played FLOAT,
    stints INTEGER,
    fg_makes INTEGER,
    fg_attempts INTEGER,
    fg_percentage FLOAT,
    pt3_makes INTEGER,
    pt3_attempts INTEGER,
    pt3_percentage FLOAT,
    ft_makes INTEGER,
    ft_attempts INTEGER,
    ft_percentage FLOAT,
    points INTEGER,
    assists INTEGER,
    orb INTEGER,
    drb INTEGER,
    rebounds INTEGER,
    fouls INTEGER,
    turnovers INTEGER,
    blocks INTEGER,
    steals INTEGER 
        
)
""")

# -------------------------
# plus minus seaonal stats
# -------------------------
cursor.execute("""
CREATE TABLE IF NOT EXISTS season_plus_minus_stats (
    player_id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_name TEXT,
    games_played INTEGER,
    plus_minus INTEGER,
    min_played FLOAT,
    plus_minus_per_40 FLOAT,
    stints INTEGER,
    plus_minus_per_stint FLOAT,
    points INTEGER,
    points_per_40 FLOAT,
    assists INTEGER,
    assists_per_40 FLOAT
    
)
""")

# -------------------------
# player stats season totals
# -------------------------
cursor.execute("""
CREATE TABLE IF NOT EXISTS player_season_stats (
    stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_name TEXT,
    games_played INTEGER,
    fg_makes INTEGER,
    fg_attempts INTEGER,
    fg_percentage FLOAT,
    pt3_makes INTEGER,
    pt3_attempts INTEGER,
    pt3_percentage FLOAT,
    ft_makes INTEGER,
    ft_attempts INTEGER,
    ft_percentage FLOAT,
    rebounds INTEGER,
    fouls INTEGER,
    turnovers INTEGER,
    blocks INTEGER,
    steals INTEGER 
        
)
""")


conn.commit()
conn.close()
 
print("Database and tables created successfully")
