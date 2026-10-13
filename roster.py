"""
roster.py
---------
Self-building, team-aware roster for UWRF players (men's AND women's).
 
PROBLEM THIS SOLVES
  Previously, parse_events.py and box_score_stats.py each kept a hardcoded
  `TEAM_PLAYERS` set - every player who had ever suited up for UWRF, typed in
  by hand, in two separate files. Every new season (recruits, transfers,
  graduations) someone had to remember to update both lists, or players would
  be silently misclassified as "Opponent".
 
HOW THIS REPLACES IT
  Box scores print the team's name on the line immediately before each
  "# Player GS MIN ..." table header, e.g.:
 
      UW-River Falls 66
      # Player GS MIN FG 3PT FT ...
 
  The school's name never changes, so it is a far better anchor than a list
  of individual names. Whenever box_score_stats.py finds the UWRF table for a
  game, every player on it is logged here as having appeared for UWRF that
  season. parse_events.py then asks "is this player on this season's UWRF
  roster?" by querying this table. A brand-new freshman is picked up the
  moment their first box score is parsed - no manual edits.
 
MEN'S vs WOMEN'S
  The box score PDFs print "UW-River Falls" for both teams and never say
  men's or women's, so every roster row is tagged with a team code:
 
      MBB = men's basketball   (the default)
      WBB = women's basketball
 
  The tag for a game is worked out automatically by get_team_for_game():
    1. if the games table has a `team` column with a value, use it;
    2. otherwise, if the game's PDF path/URL contains "women" or "wbb"
       (e.g. a folder called play_by_play_pdf_women/, or a file called
       20251108_women_vs_CentralIA.pdf), it is a women's game;
    3. otherwise it is a men's game.
 
  If the women's games live in their OWN database, nothing needs to be named
  anything - every game defaults to MBB and that database simply has its own
  roster. The naming convention only matters when both teams share one db.
"""
 
import sqlite3
from typing import Optional, Set
 
DB_PATH = "data/basketball.db"
 
# Team codes stored in the roster tables
MENS = "MBB"
WOMENS = "WBB"
 
# The one constant that stays true season over season: the school's own name,
# as it appears on the box-score team-name line.
UWRF_NAME_MARKERS = (
    "UW-RIVER FALLS",
    "UNIV. OF WISCONSIN-RIVER FALLS",
    "UNIVERSITY OF WISCONSIN-RIVER FALLS",
    "UWRF",
)
 
# If a game's PDF path/URL contains one of these, it is a women's game.
# ("MEN" is a substring of "WOMEN", so women's is always checked first.)
WOMENS_PATH_MARKERS = ("WOMEN", "WBB")
 
 
def connect_db() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)
 
 
def is_uwrf_team_label(label: Optional[str]) -> bool:
    """True if a box-score team-name line refers to UW-River Falls."""
    if not label:
        return False
    upper = label.upper()
    return any(marker in upper for marker in UWRF_NAME_MARKERS)
 
 
def ensure_roster_table(conn: sqlite3.Connection) -> None:
    """Create the roster table/view if missing, and upgrade an older roster
    table that has no `team` column (existing rows become men's, MBB)."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS roster_appearances (
            player_name TEXT NOT NULL,
            season      TEXT NOT NULL,
            game_id     INTEGER NOT NULL,
            team        TEXT NOT NULL DEFAULT 'MBB',
            PRIMARY KEY (player_name, season, game_id)
        )
    """)
    cols = [row[1] for row in cur.execute("PRAGMA table_info(roster_appearances)")]
    if "team" not in cols:
        cur.execute("ALTER TABLE roster_appearances ADD COLUMN team TEXT NOT NULL DEFAULT 'MBB'")
        cur.execute("DROP VIEW IF EXISTS roster")
    # A season's roster is just "who appeared", derived from the log above -
    # nothing to hand-maintain, and it self-corrects if a game gets re-parsed.
    cur.execute("""
        CREATE VIEW IF NOT EXISTS roster AS
        SELECT
            team,
            player_name,
            season,
            COUNT(DISTINCT game_id) AS games_played,
            MIN(game_id) AS first_game_id,
            MAX(game_id) AS last_game_id
        FROM roster_appearances
        GROUP BY team, player_name, season
    """)
    conn.commit()
 
 
def get_season_for_game(conn: sqlite3.Connection, game_id: int) -> str:
    """
    Basketball season spans two calendar years (roughly Nov-March), so it's
    labeled "YYYY-YYYY+1" using the year the season started in: games from
    Jul-Dec belong to the season that starts that year; games from Jan-Jun
    belong to the season that started the year before.
    """
    cur = conn.cursor()
    cur.execute("SELECT game_date FROM games WHERE game_id = ?", (game_id,))
    row = cur.fetchone()
    game_date = row[0] if row else None
 
    if not game_date:
        return "UNKNOWN"
 
    try:
        year, month, _ = (int(p) for p in game_date.split("-"))
    except (ValueError, AttributeError):
        return "UNKNOWN"
 
    if month >= 7:
        return f"{year}-{year + 1}"
    return f"{year - 1}-{year}"
 
 
def get_team_for_game(conn: sqlite3.Connection, game_id: int) -> str:
    """Return MBB or WBB for a game (see the MEN'S vs WOMEN'S note above)."""
    cur = conn.cursor()
 
    # 1. An explicit team column on the games table wins, if there is one.
    game_cols = [row[1] for row in cur.execute("PRAGMA table_info(games)")]
    if "team" in game_cols:
        row = cur.execute("SELECT team FROM games WHERE game_id = ?", (game_id,)).fetchone()
        if row and row[0]:
            return str(row[0]).strip().upper()
 
    # 2. Otherwise look for "women"/"wbb" in the PDF path/URL.
    row = cur.execute("SELECT source_url FROM games WHERE game_id = ?", (game_id,)).fetchone()
    path = (row[0] or "").upper() if row else ""
    if any(marker in path for marker in WOMENS_PATH_MARKERS):
        return WOMENS
 
    # 3. Default: men's.
    return MENS
 
 
def record_appearance(
    conn: sqlite3.Connection,
    player_name: str,
    season: str,
    game_id: int,
    team: str = MENS,
) -> None:
    """Log that `player_name` played for UWRF (`team`) in this game. Safe to
    call repeatedly (e.g. on re-parses of the same game) - duplicates are
    ignored via the primary key."""
    ensure_roster_table(conn)
    conn.execute(
        """
        INSERT OR IGNORE INTO roster_appearances (player_name, season, game_id, team)
        VALUES (?, ?, ?, ?)
        """,
        (player_name, season, game_id, team),
    )
    conn.commit()
 
 
def get_roster_for_season(conn: sqlite3.Connection, season: str, team: str = MENS) -> Set[str]:
    ensure_roster_table(conn)
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT player_name FROM roster_appearances WHERE season = ? AND team = ?",
        (season, team),
    )
    return {row[0] for row in cur.fetchall()}
 
 
def get_full_roster(conn: sqlite3.Connection, team: str = MENS) -> Set[str]:
    """Every player who has ever appeared for this UWRF team, any season.
    Used only as a last-resort fallback (see is_on_roster)."""
    ensure_roster_table(conn)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT player_name FROM roster_appearances WHERE team = ?", (team,))
    return {row[0] for row in cur.fetchall()}
 
 
def is_on_roster(
    conn: sqlite3.Connection,
    player_name: str,
    season: Optional[str] = None,
    team: str = MENS,
) -> bool:
    """Was this player on the UWRF roster for `team`? Checked against the
    given season first; falls back to "has this player ever appeared for this
    team, any season" only if the season lookup comes back empty."""
    ensure_roster_table(conn)
    cur = conn.cursor()
 
    if season and season != "UNKNOWN":
        cur.execute(
            "SELECT 1 FROM roster_appearances WHERE player_name = ? AND season = ? AND team = ? LIMIT 1",
            (player_name, season, team),
        )
        if cur.fetchone():
            return True
 
    cur.execute(
        "SELECT 1 FROM roster_appearances WHERE player_name = ? AND team = ? LIMIT 1",
        (player_name, team),
    )
    return cur.fetchone() is not None
