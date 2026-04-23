# parse_box_score_stats.py

import sqlite3
import re
from typing import List, Dict, Optional

DB_PATH = "basketball.db"

# -----------------------------
# UWRF ROSTER
# Used to identify which box score table belongs to UWRF
# -----------------------------
TEAM_PLAYERS = {
    "HAVLIK,OWEN",
    "THOMPSON,MICAH",
    "LEIFKER,JACK",
    "RALPH,GAVIN",
    "WANGUHU,JEREMY",
    "POSTEL,REGGIE",
    "CLEARY,DANIEL",
    "TENGBLAD,DREW",
    "TENGBLAD,REID",
    "VARIANO,BRODY",
    "CLAUSEN,EVAN",
    "LOEGERING,CODY",
    "SCHULT,WESTON",
    "JOHNSON,IAN",
    "STEINKE,KYLE",
    "ROGGE,EVAN",
}

# -----------------------------
# DATABASE
# -----------------------------
def connect_db() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def get_all_game_ids(conn: sqlite3.Connection) -> List[int]:
    cursor = conn.cursor()
    cursor.execute("SELECT game_id FROM games ORDER BY game_id")
    return [row[0] for row in cursor.fetchall()]


def get_raw_lines(conn: sqlite3.Connection, game_id: int) -> List[str]:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT line_text
        FROM raw_lines
        WHERE game_id = ?
        ORDER BY line_id
        """,
        (game_id,),
    )
    return [row[0] for row in cursor.fetchall()]


# -----------------------------
# HELPERS
# -----------------------------
def normalize_player_name(name: str) -> str:
    # Remove spaces after commas and uppercase
    name = name.strip()
    name = re.sub(r",\s+", ",", name)
    return name.upper()


def safe_pct(makes: int, attempts: int) -> float:
    if attempts == 0:
        return 0.0
    return round(makes / attempts, 4)


def parse_minutes(minutes_text: str) -> float:
    """
    Handles minute values like:
      28
      34+
      0

    The '+' appears in some PDFs and should be ignored.
    """
    cleaned = minutes_text.strip().replace("+", "")

    if cleaned == "":
        return 0.0

    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def is_team_row(parts: List[str]) -> bool:
    return parts[0] == "TM" or (len(parts) > 1 and parts[1] == "TEAM")


def parse_player_line(clean: str) -> Optional[Dict]:
    """
    Parse one player row from the box score.

    Supports rows like:
      25 Leifker,Jack * 28 5-10 0-0 8-10 1-4 5 1 2 1 0 1 18
      14 Postel, Reggie * 34+ 8-12 3-7 2-2 0-3 3 2 6 4 0 0 21

    Columns:
      #, Name, optional *, MIN, FG, 3PT, FT, ORB-DRB, REB, PF, A, TO, BLK, STL, PTS

    Names may appear as:
      Havlik,Owen
      Havlik, Owen
    """
    parts = clean.split()

    if len(parts) < 14:
        return None

    if is_team_row(parts):
        return None

    try:
        idx = 0

        # Jersey number
        if parts[idx].isdigit():
            idx += 1

        # Name may be split into two tokens if there is a space after the comma
        name = parts[idx]
        idx += 1

        # If next token is not '*' and not a minute field, it is probably the
        # second half of the player name, e.g. "Postel," + "Reggie"
        if idx < len(parts) and parts[idx] != "*" and not re.fullmatch(r"\d+\+?", parts[idx]):
            name = f"{name}{parts[idx]}"
            idx += 1

        # Starter marker
        if idx < len(parts) and parts[idx] == "*":
            idx += 1

        # Minutes field can be "28" or "34+"
        minutes = parts[idx]
        idx += 1

        fg = parts[idx]
        idx += 1

        pt3 = parts[idx]
        idx += 1

        ft = parts[idx]
        idx += 1

        orb_drb = parts[idx]
        idx += 1

        reb = parts[idx]
        idx += 1

        pf = parts[idx]
        idx += 1

        ast = parts[idx]
        idx += 1

        turnovers = parts[idx]
        idx += 1

        blk = parts[idx]
        idx += 1

        stl = parts[idx]
        idx += 1

        pts = parts[idx]
        idx += 1

        fg_m, fg_a = fg.split("-")
        pt3_m, pt3_a = pt3.split("-")
        ft_m, ft_a = ft.split("-")
        orb, drb = orb_drb.split("-")

        return {
            "player_name": normalize_player_name(name),
            "min_played": parse_minutes(minutes),

            "fg_makes": int(fg_m),
            "fg_attempts": int(fg_a),
            "fg_percentage": safe_pct(int(fg_m), int(fg_a)),

            "pt3_makes": int(pt3_m),
            "pt3_attempts": int(pt3_a),
            "pt3_percentage": safe_pct(int(pt3_m), int(pt3_a)),

            "ft_makes": int(ft_m),
            "ft_attempts": int(ft_a),
            "ft_percentage": safe_pct(int(ft_m), int(ft_a)),

            "points": int(pts),
            "assists": int(ast),
            "orb": int(orb),
            "drb": int(drb),
            "rebounds": int(reb),
            "fouls": int(pf),
            "turnovers": int(turnovers),
            "blocks": int(blk),
            "steals": int(stl),
        }

    except Exception as e:
        print(f"\nError parsing line: {clean}")
        print(e)
        return None


# -----------------------------
# BOX SCORE TABLE EXTRACTION
# -----------------------------
def extract_box_score_tables(lines: List[str]) -> List[List[Dict]]:
    """
    Extract full-game player tables only.

    This skips:
      - 1st Half Box Score
      - 2nd Half Box Score

    and only keeps the first two full-game tables near the top of the PDF.
    """
    tables: List[List[Dict]] = []
    current_table: List[Dict] = []
    in_table = False

    reached_half_box_scores = False

    for raw_line in lines:
        clean = raw_line.strip()

        # Stop before half box score sections
        if clean.startswith("1st Half Box Score") or clean.startswith("2nd Half Box Score"):
            reached_half_box_scores = True

        if reached_half_box_scores:
            break

        # Start of a full-game box score table
        if clean.startswith("# Player GS MIN"):
            if in_table and current_table:
                tables.append(current_table)
            current_table = []
            in_table = True
            continue

        if not in_table:
            continue

        # End of current table
        if clean.startswith("Totals"):
            if current_table:
                tables.append(current_table)
            current_table = []
            in_table = False
            continue

        player = parse_player_line(clean)
        if player is not None:
            current_table.append(player)

    if in_table and current_table:
        tables.append(current_table)

    return tables


def count_roster_matches(table: List[Dict]) -> int:
    return sum(1 for player in table if player["player_name"] in TEAM_PLAYERS)


def select_uwrf_table(tables: List[List[Dict]]) -> List[Dict]:
    """
    Pick the table with the most UWRF roster-name matches.
    This handles home and away games automatically.
    """
    if not tables:
        return []

    best_table = max(tables, key=count_roster_matches)
    best_match_count = count_roster_matches(best_table)

    if best_match_count == 0:
        print("WARNING: No UWRF roster matches found in any full-game box score table.")
        return []

    return best_table


def parse_box_score(lines: List[str]) -> List[Dict]:
    tables = extract_box_score_tables(lines)

    if not tables:
        print("WARNING: No full-game box score tables found.")
        return []

    return select_uwrf_table(tables)


# -----------------------------
# UPDATE DATABASE
# -----------------------------
def update_player_stats(conn: sqlite3.Connection, game_id: int, players: List[Dict]) -> None:
    cursor = conn.cursor()

    for p in players:
        cursor.execute(
            """
            UPDATE player_game_stats
            SET
                min_played = ?,
                fg_makes = ?, fg_attempts = ?, fg_percentage = ?,
                pt3_makes = ?, pt3_attempts = ?, pt3_percentage = ?,
                ft_makes = ?, ft_attempts = ?, ft_percentage = ?,
                points = ?, assists = ?,
                orb = ?, drb = ?, rebounds = ?,
                fouls = ?, turnovers = ?, blocks = ?, steals = ?
            WHERE game_id = ? AND player_name = ?
            """,
            (
                p["min_played"],
                p["fg_makes"], p["fg_attempts"], p["fg_percentage"],
                p["pt3_makes"], p["pt3_attempts"], p["pt3_percentage"],
                p["ft_makes"], p["ft_attempts"], p["ft_percentage"],
                p["points"], p["assists"],
                p["orb"], p["drb"], p["rebounds"],
                p["fouls"], p["turnovers"], p["blocks"], p["steals"],
                game_id,
                p["player_name"],
            ),
        )

    conn.commit()


# -----------------------------
# RUN FOR ONE GAME
# -----------------------------
def parse_game_box_score(game_id: int) -> None:
    conn = connect_db()

    print(f"\nProcessing game_id: {game_id}")

    lines = get_raw_lines(conn, game_id)
    players = parse_box_score(lines)

    print(f"Parsed {len(players)} UWRF players\n")

    for p in players:
        print(
            f"{p['player_name']}  "
            f"MIN: {p['min_played']}  "
            f"PTS: {p['points']}  "
            f"REB: {p['rebounds']}  "
            f"FG%: {p['fg_percentage']:.4f}  "
            f"3PT%: {p['pt3_percentage']:.4f}  "
            f"FT%: {p['ft_percentage']:.4f}"
        )

    update_player_stats(conn, game_id, players)
    conn.close()


# -----------------------------
# RUN FOR ALL GAMES
# -----------------------------
def parse_all_games() -> None:
    conn = connect_db()
    game_ids = get_all_game_ids(conn)
    conn.close()

    print(f"Found {len(game_ids)} games\n")

    for game_id in game_ids:
        parse_game_box_score(game_id)


# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    # Single game
    #parse_game_box_score(1)

    # All games
     parse_all_games()
