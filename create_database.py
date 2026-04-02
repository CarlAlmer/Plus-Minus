import sqlite3

# this will create the database file if it doesn't exist
conn = sqlite3.connect("data/basketball.db")

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
# lineup states
# -------------------------
cursor.execute("""
CREATE TABLE IF NOT EXISTS lineup_states (
    lineup_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id INTEGER,
    event_num INTEGER,
    player1 TEXT,
    player2 TEXT,
    player3 TEXT,
    player4 TEXT,
    player5 TEXT
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
    plus_minus INTEGER
)
""")

conn.commit()
conn.close()

print("Database and tables created successfully")
