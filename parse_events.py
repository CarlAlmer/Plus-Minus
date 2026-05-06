import sqlite3
import re
from itertools import groupby
from typing import List, Dict, Optional, Tuple
 
 
# Path to the local SQLite database
DB_PATH = "data/basketball.db"
 
# Known UWRF roster — used to identify which team a player belongs to
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
    "NELSON,RILEY",
    "COGDILL,NOLAN",
    "HERICKHOFF,CHAD"
}
 
# ---------------------------------------------------------------------------
# Event priority map — controls ordering within identical (period, clock) groups.
#
# Problem this solves:
#   The PDF sometimes records a substitution at the same timestamp as a free
#   throw.  If the sub is processed first, the incoming player gets credited
#   for a point he was never on the floor for.
#
# Solution:
#   Within any group of events sharing the same (period, clock), always
#   process scoring events first and substitutions last.  The numbers below
#   are sort keys — lower = processed earlier.
#
#   Priority 1 → scoring plays  (must settle plus/minus before the lineup changes)
#   Priority 2 → all other game events  (rebounds, fouls, turnovers, etc.)
#   Priority 3 → substitutions  (lineup changes happen after all scoring is done)
# ---------------------------------------------------------------------------
EVENT_PRIORITY = {
    # --- Scoring events: processed first ---
    "made_ft":          1,   # Free throw made — point value must hit the current lineup
    "miss_ft":          1,   # Free throw missed — still a floor event, keep with scoring
    "made_2pt":         1,   # Field goal made (2-pointer)
    "miss_2pt":         1,   # Field goal missed (2-pointer)
    "made_3pt":         1,   # Field goal made (3-pointer)
    "miss_3pt":         1,   # Field goal missed (3-pointer)
 
    # --- Other game events: processed second ---
    "rebound_def":      2,   # Defensive rebound
    "rebound_off":      2,   # Offensive rebound
    "deadball_rebound": 2,   # Deadball / team rebound
    "foul":             2,   # Personal or technical foul
    "turnover":         2,   # Turnover
    "steal":            2,   # Steal
    "block":            2,   # Block
    "assist":           2,   # Assist
    "timeout":          2,   # Timeout (team or media)
    "jump_ball":        2,   # Jump ball / tip-off
    "other":            2,   # Catch-all for anything unclassified
 
    # --- Substitutions: processed last ---
    # This is the key fix: subs always trail every scoring event at the same timestamp,
    # so the player who earned the trip to the line receives the plus/minus credit.
    "sub_in":           4,   # Player entering the game
    "sub_out":          3,   # Player leaving the game
}
 
 
def connect_db() -> sqlite3.Connection:
    # Open a connection to the SQLite database at DB_PATH
    return sqlite3.connect(DB_PATH)
 
 
def get_raw_lines_for_game(conn: sqlite3.Connection, game_id: int) -> List[str]:
    # Pull every raw PDF line for this game, in the order it was imported
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
    rows = cursor.fetchall()
    # Return just the text of each line as a plain list of strings
    return [row[0] for row in rows]
 
 
def delete_existing_events_for_game(conn: sqlite3.Connection, game_id: int) -> None:
    # Wipe any previously parsed events for this game so we start clean
    cursor = conn.cursor()
    cursor.execute("DELETE FROM events WHERE game_id = ?", (game_id,))
    conn.commit()
 
 
def normalize_spaces(text: str) -> str:
    # Collapse any run of whitespace (tabs, double spaces) into a single space
    return re.sub(r"\s+", " ", text).strip()
 
 
def normalize_player_name(name: Optional[str]) -> Optional[str]:
    # Standardize player names to LASTNAME,FIRSTNAME (no space after comma, all caps)
    if not name:
        return None
    return re.sub(r",\s+", ",", name.strip().upper())
 
 
def identify_team(player: Optional[str]) -> Optional[str]:
    # Check whether a player is on the UWRF roster; if not, treat them as Opponent
    player = normalize_player_name(player)
    if not player:
        return None
    if player in TEAM_PLAYERS:
        return "UWRF"
    return "Opponent"
 
 
def should_skip_line(line: str) -> bool:
    # Return True for header/metadata lines that contain no play-by-play events
    clean = line.strip()
    if not clean:
        # Always skip completely empty lines
        return True
 
    upper_clean = clean.upper()
 
    # These prefixes identify section headers, box score rows, and game notes —
    # none of them contain events we need to parse
    skip_prefixes = (
        "VISITORS:",
        "HOME TEAM:",
        "GAME NOTES",
        "OFFICIALS:",
        "REFEREES:",
        "DATE:",
        "TIME:",
        "SITE:",
        "ATTENDANCE:",
        "# PLAYER",
        "TEAM SUMMARY",
        "TECHNICAL FOULS:",
        "LEAD CHANGED:",
        "POINTS IN PAINT",
        "POINTS OFF TURNOVERS",
        "2ND CHANCE POINTS",
        "FASTBREAK POINTS",
        "BENCH POINTS",
        "SCORE TIED",
        "LAST FG -",
        "LARGEST LEAD -",
        "FIRST HALF",
        "SECOND HALF",
        "TOTAL ",
    )
 
    return upper_clean.startswith(skip_prefixes)
 
 
def classify_event_type(lower_line: str) -> Tuple[str, int]:
    # Map the lowercased line text to an (event_type, point_value) pair.
    # The event_type strings here must match the keys in EVENT_PRIORITY above.
 
    # --- Made field goals ---
    if "good layup" in lower_line or "good jumper" in lower_line or "good dunk" in lower_line or "good tipin" in lower_line:
        return "made_2pt", 2
    if "good 3ptr" in lower_line or "good 3-pt" in lower_line:
        return "made_3pt", 3
    if "good ft" in lower_line or "good free throw" in lower_line:
        return "made_ft", 1
 
    # --- Missed shots ---
    if "miss layup" in lower_line or "miss jumper" in lower_line or "miss dunk" in lower_line:
        return "miss_2pt", 0
    if "miss 3ptr" in lower_line or "miss 3-pt" in lower_line:
        return "miss_3pt", 0
    if "miss ft" in lower_line or "miss free throw" in lower_line:
        return "miss_ft", 0
 
    # --- Substitutions ---
    if "sub in by" in lower_line:
        return "sub_in", 0
    if "sub out by" in lower_line:
        return "sub_out", 0
 
    # --- Other game events ---
    if "turnover by" in lower_line:
        return "turnover", 0
    if "foul by" in lower_line:
        return "foul", 0
    if "rebound def by" in lower_line:
        return "rebound_def", 0
    if "rebound off by" in lower_line:
        return "rebound_off", 0
    if "deadball rebound" in lower_line:
        return "deadball_rebound", 0
    if "steal by" in lower_line:
        return "steal", 0
    if "block by" in lower_line:
        return "block", 0
    if "assist by" in lower_line:
        return "assist", 0
    if "timeout" in lower_line:
        return "timeout", 0
    if "jump ball" in lower_line or "jumpball" in lower_line:
        return "jump_ball", 0
 
    # Catch-all for lines that matched looks_like_event_content but nothing specific above
    return "other", 0
 
 
def extract_clock(line: str) -> Optional[str]:
    # Pull the first MM:SS timestamp found in the line (e.g. "15:32")
    match = re.search(r"(\d{1,2}:\d{2})", line)
    if match:
        return match.group(1)
    return None
 
 
def extract_player(line: str) -> Optional[str]:
    # Find the player name following a "by" keyword (e.g. "Good FT by HAVLIK,OWEN")
    match = re.search(r"\bby\s+([A-Z,\-'.]+(?:\([^)]+\))?)", line, re.IGNORECASE)
    if not match:
        return None
 
    player_text = match.group(1).strip()
    # Strip any parenthetical annotations (e.g. "(2nd foul)") from the name
    player_text = re.sub(r"\(.*?\)", "", player_text).strip()
    return normalize_player_name(player_text)
 
 
def looks_like_event_content(line: str) -> bool:
    # Quick pre-filter: only spend time parsing lines that contain at least one
    # event keyword.  Lines without any of these are stats rows or headers.
    lower_line = line.lower()
    keywords = (
        "good ",
        "miss ",
        "sub in by",
        "sub out by",
        "turnover by",
        "foul by",
        "rebound def by",
        "rebound off by",
        "deadball rebound",
        "steal by",
        "block by",
        "assist by",
        "timeout",
        "jump ball",
        "jumpball",
    )
    return any(k in lower_line for k in keywords)
 
 
def parse_line(line: str, inherited_clock: Optional[str]) -> Dict:
    # Convert a single raw PDF line into an event dictionary (no event_num yet —
    # that is assigned later, after sorting)
    clean_line = normalize_spaces(line)
    lower_line = clean_line.lower()
 
    event_type, points = classify_event_type(lower_line)
    player = extract_player(clean_line)
 
    # Use the timestamp on this line; fall back to the last seen clock if absent
    clock = extract_clock(clean_line)
    if not clock:
        clock = inherited_clock
 
    return {
        "clock":       clock,
        "period":      None,        # Will be filled in by the caller from context
        "team":        identify_team(player),
        "player":      player,
        "event_type":  event_type,
        "points":      points,
        "description": clean_line,
    }
 
 
# ---------------------------------------------------------------------------
# Priority sort — the core fix
# ---------------------------------------------------------------------------
def sort_events_by_priority(events: List[Dict]) -> List[Dict]:
    """
    Within each (period, clock) group, re-order events so that:
      1. Scoring plays come first
      2. Other game events come second
      3. Substitutions come last
 
    Events in different groups keep their original relative order (the sort
    is stable, so PDF order is preserved within the same priority tier).
 
    After sorting, event_num is re-assigned sequentially so the database
    reflects the corrected order.  compute_plus_minus.py uses ORDER BY
    event_num, so fixing the numbers here is all that is needed — no changes
    to that script required.
    """
 
    # --- Step 1: Group consecutive events that share the same (period, clock) ---
    # We use groupby on a key tuple.  Because the input is already ordered by
    # PDF line number (i.e. period/clock already monotonically increases),
    # groupby correctly buckets same-timestamp events together.
    sorted_events: List[Dict] = []
 
    for _key, group in groupby(events, key=lambda e: (e["period"], e["clock"])):
        # Collect all events in this timestamp bucket into a list
        group_list = list(group)
 
        # --- Step 2: Stable-sort the bucket by event priority ---
        # sorted() is guaranteed stable in Python, so events with the same
        # priority keep their original PDF order relative to each other.
        # Events not found in EVENT_PRIORITY default to priority 2 (middle tier)
        # so genuinely unknown events don't accidentally land before scoring plays.
        group_list.sort(key=lambda e: EVENT_PRIORITY.get(e["event_type"], 2))
 
        # Append the correctly ordered bucket to the master list
        sorted_events.extend(group_list)
 
    # --- Step 3: Re-assign event_num based on the new sorted order ---
    # event_num now reflects the corrected processing order, not PDF line order.
    # This is the number that compute_plus_minus.py uses to iterate events,
    # so writing it correctly here means the compute script needs zero changes.
    for i, event in enumerate(sorted_events, start=1):
        event["event_num"] = i
 
    return sorted_events
 
 
def insert_parsed_events(conn: sqlite3.Connection, parsed_events: List[Dict]) -> None:
    # Write each parsed (and now correctly ordered) event into the events table
    cursor = conn.cursor()
 
    for event in parsed_events:
        cursor.execute(
            """
            INSERT INTO events (
                game_id,
                event_num,
                period,
                clock,
                team,
                player,
                event_type,
                points,
                description
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event["game_id"],
                event["event_num"],   # Now the priority-corrected number
                event["period"],
                event["clock"],
                event["team"],
                event["player"],
                event["event_type"],
                event["points"],
                event["description"],
            ),
        )
 
    conn.commit()
 
 
def parse_game_events(game_id: int, delete_existing: bool = True) -> None:
    conn = connect_db()
 
    if delete_existing:
        # Remove old events so re-importing a game doesn't create duplicates
        delete_existing_events_for_game(conn, game_id)
 
    # Load every raw PDF line for this game from the database
    raw_lines = get_raw_lines_for_game(conn, game_id)
 
    # Accumulate parsed events here before sorting; event_num is NOT set yet
    parsed_events: List[Dict] = []
 
    current_period: Optional[int] = None   # Tracks which half/OT we are in
    in_pbp_section: bool = False            # True once we hit a play-by-play header
    current_clock: Optional[str] = None    # Last clock value seen (used as fallback)
 
    # ---------------------------------------------------------------------------
    # Pre-process: merge lines that the PDF wrapped mid-event.
    # This happens when a long player name pushes text onto the next line,
    # e.g. 'GOOD LAYUP by' followed by 'SCHWARZENBERGER,DOMA(fastbreak)...'
    # We detect any line that ends with ' by' and join it with the next line
    # so the parser always sees a complete event on a single string.
    # ---------------------------------------------------------------------------
    merged_lines = []
    i = 0
    while i < len(raw_lines):
        line = raw_lines[i]
        if normalize_spaces(line).endswith(" by") and i + 1 < len(raw_lines):
            line = normalize_spaces(line) + " " + normalize_spaces(raw_lines[i + 1])
            i += 2
        else:
            i += 1
        merged_lines.append(line)
    raw_lines = merged_lines
 
    for raw_line in raw_lines:
        clean = normalize_spaces(raw_line)
 
        # Detect half/OT section headers and update period context
        if clean == "1st Half Play By Play":
            current_period = 1
            in_pbp_section = True
            current_clock = None   # Reset clock at the start of each half
            continue
 
        if clean == "2nd Half Play By Play":
            current_period = 2
            in_pbp_section = True
            current_clock = None
            continue
 
        if clean == "OT Play By Play":
            current_period = 3
            in_pbp_section = True
            current_clock = None
            continue
 
        # Skip everything before the first play-by-play section header
        if not in_pbp_section:
            continue
 
        # Stop parsing once we hit the game notes or officials block
        if clean.startswith("Game Notes") or clean.startswith("Officials:"):
            break
 
        # Skip header / metadata lines that don't contain events
        if should_skip_line(clean):
            continue
 
        # Skip lines that don't contain any event keywords
        if not looks_like_event_content(clean):
            continue
 
        # Update the inherited clock whenever this line contains a timestamp
        line_clock = extract_clock(clean)
        if line_clock:
            current_clock = line_clock
 
        # Parse the line into an event dict (event_num not set yet)
        event = parse_line(clean, current_clock)
 
        # Discard events with no resolvable clock — we can't place them in order
        if event["clock"] is None:
            continue
 
        # Attach game-level context that parse_line doesn't have
        event["game_id"] = game_id
        event["period"]  = current_period
        # Note: event_num is intentionally omitted here; sort_events_by_priority sets it
        parsed_events.append(event)
 
    # Apply priority sort and re-assign event_num
    parsed_events = sort_events_by_priority(parsed_events)

    # Write the sorted, correctly numbered events to the database
    insert_parsed_events(conn, parsed_events)
    conn.close()


def get_unparsed_game_ids(conn: sqlite3.Connection) -> List[int]:
    # Return every game_id that has raw lines loaded but no events parsed yet.
    # This lets __main__ automatically pick up any game that bulk_import.py
    # has imported but parse_game_events has not yet run on.
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT DISTINCT r.game_id
        FROM raw_lines r
        LEFT JOIN events e ON e.game_id = r.game_id
        WHERE e.game_id IS NULL
        ORDER BY r.game_id
        """
    )
    return [row[0] for row in cursor.fetchall()]


if __name__ == "__main__":
    # Automatically parse every game that has raw lines but no events yet.
    # No game_id needs to be specified — this picks up whatever bulk_import.py
    # has loaded since the last time this script was run.
    conn = connect_db()
    unparsed_ids = get_unparsed_game_ids(conn)
    conn.close()

    for game_id in unparsed_ids:
        parse_game_events(game_id=game_id, delete_existing=True)
