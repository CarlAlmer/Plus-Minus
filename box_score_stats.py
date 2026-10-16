# parse_box_score_stats.py

import sqlite3
import re
from typing import List, Dict, Optional, Tuple

import roster

DB_PATH = "data/basketball.db"

# NOTE: the old hardcoded TEAM_PLAYERS name list is gone. Which box score
# table belongs to UWRF is now determined by the team-name label the PDF
# prints right above each table (see select_uwrf_table below), and every
# player found on that table is logged to roster_appearances automatically -
# see roster.py for why that's a more durable approach across seasons.

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

        # The name is every token up to the '*' / minutes: "Havlik,Owen", "Postel, Reggie",
        # "Reeck ,Jake" (space before the comma) or "Van Scoy,Will" (two-word last name)
        name_tokens = [parts[idx]]
        idx += 1
        while idx < len(parts) and parts[idx] != "*" and not re.fullmatch(r"\d+\+?", parts[idx]):
            name_tokens.append(parts[idx])
            idx += 1
        name = re.sub(r"\s*,\s*", ",", " ".join(name_tokens))

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
def extract_box_score_tables(lines: List[str]) -> List[Tuple[str, List[Dict]]]:
    """
    Extract full-game player tables only, each paired with the team-name
    label the PDF prints on the line immediately before the table header,
    e.g.:
        UW-River Falls 66
        # Player GS MIN FG 3PT FT ...

    That label is what select_uwrf_table uses to identify UWRF's table -
    no player-name list required.

    This skips:
      - 1st Half Box Score
      - 2nd Half Box Score

    and only keeps the first two full-game tables near the top of the PDF.
    """
    tables: List[Tuple[str, List[Dict]]] = []
    current_table: List[Dict] = []
    current_label = ""
    prev_line = ""
    in_table = False

    reached_half_box_scores = False

    for raw_line in lines:
        clean = raw_line.strip()

        # Stop before half box score sections
        if clean.startswith(("1st Half Box Score", "2nd Half Box Score", "First Half Box Score", "Second Half Box Score")):
            reached_half_box_scores = True

        if reached_half_box_scores:
            break

        # Start of a full-game box score table - the team name is on the
        # line right before this header
        if clean.startswith("# Player GS MIN"):
            if in_table and current_table:
                tables.append((current_label, current_table))
            current_table = []
            current_label = prev_line
            in_table = True
            prev_line = clean
            continue

        if not in_table:
            prev_line = clean
            continue

        # End of current table
        if clean.startswith("Totals"):
            if current_table:
                tables.append((current_label, current_table))
            current_table = []
            in_table = False
            prev_line = clean
            continue

        player = parse_player_line(clean)
        if player is not None:
            current_table.append(player)

        prev_line = clean

    if in_table and current_table:
        tables.append((current_label, current_table))

    return tables


def select_uwrf_table(
    conn: sqlite3.Connection,
    game_id: int,
    tables: List[Tuple[str, List[Dict]]],
) -> List[Dict]:
    """
    Pick the table whose team-name label identifies it as UWRF's (handles
    home and away automatically, since the label always sits right above
    the table regardless of order). Every player found on that table is
    then logged to the self-building roster for this game's season.
    """
    if not tables:
        return []

    matches = [(label, table) for label, table in tables if roster.is_uwrf_team_label(label)]

    if not matches:
        print(
            "WARNING: Could not find a UWRF-labeled box score table "
            f"(labels seen: {[label for label, _ in tables]!r}). "
            "Falling back to whichever table has the most known-roster names."
        )
        known = roster.get_full_roster(conn, roster.get_team_for_game(conn, game_id))
        best_label, best_table = max(
            tables,
            key=lambda pair: sum(1 for p in pair[1] if p["player_name"] in known),
        )
        if not any(p["player_name"] in known for p in best_table):
            print("WARNING: No UWRF roster matches found in any full-game box score table either.")
            return []
        matches = [(best_label, best_table)]

    _, uwrf_table = matches[0]

    season = roster.get_season_for_game(conn, game_id)
    team = roster.get_team_for_game(conn, game_id)   # MBB or WBB
    for player in uwrf_table:
        roster.record_appearance(conn, player["player_name"], season, game_id, team)

    return uwrf_table


def parse_box_score(conn: sqlite3.Connection, game_id: int, lines: List[str]) -> List[Dict]:
    tables = extract_box_score_tables(lines)

    if not tables:
        print("WARNING: No full-game box score tables found.")
        return []

    return select_uwrf_table(conn, game_id, tables)


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
    players = parse_box_score(conn, game_id, lines)

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
# ROSTER-ONLY PASS
#
# This only reads raw_lines and writes to roster_appearances - it does not
# touch player_game_stats, so it can run early in the pipeline (right after
# import, before compute_plus_minus has created those rows). run_all.py
# runs this before parse_events, so identify_team() always has this
# season's roster available by the time it needs it.
# -----------------------------
def build_roster_for_all_games() -> None:
    conn = connect_db()
    game_ids = get_all_game_ids(conn)

    print(f"Building roster from {len(game_ids)} games' box scores...\n")

    for game_id in game_ids:
        lines = get_raw_lines(conn, game_id)
        players = parse_box_score(conn, game_id, lines)
        print(f"  game_id {game_id}: {len(players)} UWRF players logged to roster")

    conn.close()
    print("\nRoster build complete.")


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

    load_games_without_play_by_play()


def load_games_without_play_by_play() -> None:
    """Some box score PDFs have no play-by-play section (it was never posted).
    Plus/minus, stints and lineups can't be worked out for those games, but the
    box score numbers can still be kept. For each such game: remove anything the
    play-by-play steps may have half-built, then store one row per UWRF player
    with the box score stats and plus_minus / stints left empty (NULL)."""
    conn = connect_db()
    try:
        game_ids = [
            gid for gid in get_all_game_ids(conn)
            if not any("play by play" in l.lower() for l in get_raw_lines(conn, gid))
        ]
        for gid in game_ids:
            players = parse_box_score(conn, gid, get_raw_lines(conn, gid))
            for table in ("lineup_states", "events", "player_game_stats"):
                conn.execute(f"DELETE FROM {table} WHERE game_id = ?", (gid,))
            for p in players:
                conn.execute(
                    "INSERT INTO player_game_stats (game_id, player_name, plus_minus, stints) "
                    "VALUES (?, ?, NULL, NULL)",
                    (gid, p["player_name"]),
                )
            conn.commit()
            update_player_stats(conn, gid, players)
            print(f"  game_id {gid}: no play-by-play in the PDF - kept {len(players)} players' "
                  "box score stats, plus/minus left empty")
    finally:
        conn.close()


# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    # Single game
    #parse_game_box_score(1)

    # All games
     parse_all_games()