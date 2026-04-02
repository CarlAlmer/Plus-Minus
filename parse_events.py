import sqlite3
import re
from typing import List, Dict, Optional, Tuple


DB_PATH = "data/basketball.db"

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


def connect_db() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def get_raw_lines_for_game(conn: sqlite3.Connection, game_id: int) -> List[str]:
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
    return [row[0] for row in rows]


def delete_existing_events_for_game(conn: sqlite3.Connection, game_id: int) -> None:
    cursor = conn.cursor()
    cursor.execute("DELETE FROM events WHERE game_id = ?", (game_id,))
    conn.commit()


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def normalize_player_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    return re.sub(r",\s+", ",", name.strip().upper())


def identify_team(player: Optional[str]) -> Optional[str]:
    player = normalize_player_name(player)
    if not player:
        return None
    if player in TEAM_PLAYERS:
        return "UWRF"
    return "Opponent"


def should_skip_line(line: str) -> bool:
    clean = line.strip()
    if not clean:
        return True

    upper_clean = clean.upper()

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
    if "good layup" in lower_line or "good jumper" in lower_line or "good dunk" in lower_line or "good tipin" in lower_line:
        return "made_2pt", 2
    if "good 3ptr" in lower_line or "good 3-pt" in lower_line:
        return "made_3pt", 3
    if "good ft" in lower_line or "good free throw" in lower_line:
        return "made_ft", 1

    if "miss layup" in lower_line or "miss jumper" in lower_line or "miss dunk" in lower_line:
        return "miss_2pt", 0
    if "miss 3ptr" in lower_line or "miss 3-pt" in lower_line:
        return "miss_3pt", 0
    if "miss ft" in lower_line or "miss free throw" in lower_line:
        return "miss_ft", 0

    if "sub in by" in lower_line:
        return "sub_in", 0
    if "sub out by" in lower_line:
        return "sub_out", 0
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

    return "other", 0


def extract_clock(line: str) -> Optional[str]:
    match = re.search(r"(\d{1,2}:\d{2})", line)
    if match:
        return match.group(1)
    return None


def extract_player(line: str) -> Optional[str]:
    match = re.search(r"\bby\s+([A-Z,\-'.]+(?:\([^)]+\))?)", line, re.IGNORECASE)
    if not match:
        return None

    player_text = match.group(1).strip()
    player_text = re.sub(r"\(.*?\)", "", player_text).strip()
    return normalize_player_name(player_text)


def looks_like_event_content(line: str) -> bool:
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
    clean_line = normalize_spaces(line)
    lower_line = clean_line.lower()

    event_type, points = classify_event_type(lower_line)
    player = extract_player(clean_line)

    clock = extract_clock(clean_line)
    if not clock:
        clock = inherited_clock

    return {
        "clock": clock,
        "period": None,
        "team": identify_team(player),
        "player": player,
        "event_type": event_type,
        "points": points,
        "description": clean_line,
    }


def insert_parsed_events(conn: sqlite3.Connection, parsed_events: List[Dict]) -> None:
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
                event["event_num"],
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


def parse_game_events(game_id: int, delete_existing: bool = True, preview_count: int = 30) -> None:
    conn = connect_db()

    if delete_existing:
        delete_existing_events_for_game(conn, game_id)

    raw_lines = get_raw_lines_for_game(conn, game_id)

    parsed_events: List[Dict] = []
    event_num = 1
    current_period: Optional[int] = None
    in_pbp_section = False
    current_clock: Optional[str] = None

    for raw_line in raw_lines:
        clean = normalize_spaces(raw_line)

        if clean == "1st Half Play By Play":
            current_period = 1
            in_pbp_section = True
            current_clock = None
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

        if not in_pbp_section:
            continue

        if clean.startswith("Game Notes") or clean.startswith("Officials:"):
            break

        if should_skip_line(clean):
            continue

        if not looks_like_event_content(clean):
            continue

        line_clock = extract_clock(clean)
        if line_clock:
            current_clock = line_clock

        event = parse_line(clean, current_clock)

        if event["clock"] is None:
            continue

        event["game_id"] = game_id
        event["event_num"] = event_num
        event["period"] = current_period
        parsed_events.append(event)
        event_num += 1

    print(f"Found {len(parsed_events)} parsed events for game_id {game_id}\n")

    for event in parsed_events[:preview_count]:
        print(event)

    insert_parsed_events(conn, parsed_events)
    conn.close()

    print(f"\nParsed and inserted {len(parsed_events)} events for game_id {game_id}")


if __name__ == "__main__":
    game_id = 3
    parse_game_events(game_id=game_id, delete_existing=True, preview_count=30)