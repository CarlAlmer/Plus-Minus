import sqlite3
import re
from typing import Dict, List, Optional, Tuple
 
# could also need path to be data/basketball.db if running from a different directory
DB_PATH = "data/basketball.db"
 
# -------------------------------------------------------------------------
# DB CONNECTION
# -------------------------------------------------------------------------
def connect_db() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)
 
# -------------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------------
def normalize_player_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    return re.sub(r",\s+", ",", name.strip().upper())
 
 
# -------------------------------------------------------------------------
# Starting lineup + score — parsed automatically from the raw box score
# -------------------------------------------------------------------------
 
def parse_starters_and_scores(lines: List[str]):
    """
    Read the raw PDF lines for a game and return:
      - starters: list of 5 UWRF player name strings (LAST,FIRST format)
      - uwrf_score: UWRF final score (int)
      - opp_score:  opponent final score (int)
 
    Starters are identified by the asterisk (*) in the UWRF box score section.
    Scores are read from the score-by-period line embedded in the raw text.
    """
    uwrf_score = None
    opp_score = None
    starters = []
    in_uwrf_box = False
 
    for line in lines:
        # Score is on a line like: 'Site: ... UW-River Falls 32 38 70'
        score_match = re.search(r"UW-River Falls\s+\d+\s+\d+\s+(\d+)", line)
        if score_match and uwrf_score is None:
            uwrf_score = int(score_match.group(1))
 
        # Opponent score: a line matching 'Team Name X Y TOTAL' before UWRF section
        if not in_uwrf_box and opp_score is None:
            opp_match = re.match(r"^[A-Za-z\.\-\s\(\)]+\s+\d+\s+\d+\s+(\d+)$", line.strip())
            if opp_match and "UW-River Falls" not in line:
                opp_score = int(opp_match.group(1))
 
        # UWRF box score section starts with exactly 'UW-River Falls <score>'
        if re.match(r"^UW-River Falls\s+\d+$", line.strip()):
            in_uwrf_box = True
            continue
 
        if in_uwrf_box:
            if line.strip().startswith("Totals"):
                break
            # Starter lines contain an asterisk: '03 Havlik,Owen * 33 ...'
            match = re.match(r"^\d+\s+([\w,\-\'\.]+(?:\s+[\w\-\'\.]+)?)\s+\*", line.strip())
            if match:
                starters.append(normalize_player_name(match.group(1).strip()))
 
    return starters, uwrf_score, opp_score
 
 
def get_starters_and_scores(conn: sqlite3.Connection, game_id: int):
    cursor = conn.cursor()
    cursor.execute(
        "SELECT line_text FROM raw_lines WHERE game_id = ? ORDER BY line_id",
        (game_id,),
    )
    lines = [row[0] for row in cursor.fetchall()]
    return parse_starters_and_scores(lines)
 
 
# -------------------------------------------------------------------------
# Database helpers
# -------------------------------------------------------------------------
 
def get_events_for_game(conn: sqlite3.Connection, game_id: int) -> List[Tuple]:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT event_num, period, clock, team, player, event_type, points, description
        FROM events
        WHERE game_id = ?
        ORDER BY event_num
        """,
        (game_id,),
    )
    return cursor.fetchall()
 
 
def clear_existing_lineup_states(conn: sqlite3.Connection, game_id: int) -> None:
    cursor = conn.cursor()
    cursor.execute("DELETE FROM lineup_states WHERE game_id = ?", (game_id,))
    conn.commit()
 
 
def clear_existing_player_stats(conn: sqlite3.Connection, game_id: int) -> None:
    cursor = conn.cursor()
    cursor.execute("DELETE FROM player_game_stats WHERE game_id = ?", (game_id,))
    conn.commit()
 
 
def get_or_create_lineup_id(conn: sqlite3.Connection, lineup: List[str]) -> Optional[int]:
    """
    Return the lineup_id for this exact 5-man group, creating a new row in
    the lineups table if this combination has not been seen before.
 
    Players are sorted alphabetically before storage so that the same group
    always maps to the same lineup_id regardless of the order they appear in
    the live lineup list.
 
    Returns None if fewer than 5 players are provided.
    """
    if len(lineup) != 5:
        return None
 
    sorted_players = sorted(lineup)
    cursor = conn.cursor()
 
    cursor.execute(
        """
        SELECT lineup_id FROM lineups
        WHERE player1 = ? AND player2 = ? AND player3 = ? AND player4 = ? AND player5 = ?
        """,
        tuple(sorted_players),
    )
    row = cursor.fetchone()
    if row:
        return row[0]
 
    cursor.execute(
        """
        INSERT INTO lineups (player1, player2, player3, player4, player5)
        VALUES (?, ?, ?, ?, ?)
        """,
        tuple(sorted_players),
    )
    return cursor.lastrowid
 
 
def insert_lineup_state(
    conn: sqlite3.Connection,
    game_id: int,
    event_num: int,
    lineup: List[str],
) -> None:
    padded = lineup[:5] + [None] * (5 - len(lineup[:5]))
    lineup_id = get_or_create_lineup_id(conn, lineup)
 
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO lineup_states (
            game_id, event_num, player1, player2, player3, player4, player5, lineup_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            game_id,
            event_num,
            padded[0],
            padded[1],
            padded[2],
            padded[3],
            padded[4],
            lineup_id,
        ),
    )
 
 
def insert_player_game_stats(
    conn: sqlite3.Connection,
    game_id: int,
    plus_minus: Dict[str, int],
) -> None:
    cursor = conn.cursor()
    for player in sorted(plus_minus.keys()):
        cursor.execute(
            """
            INSERT INTO player_game_stats (game_id, player_name, plus_minus)
            VALUES (?, ?, ?)
            """,
            (game_id, player, plus_minus[player]),
        )
    conn.commit()
 
 
def ensure_player(plus_minus: Dict[str, int], player: Optional[str]) -> None:
    if player and player not in plus_minus:
        plus_minus[player] = 0
 
 
def apply_scoring_event(
    lineup: List[str],
    plus_minus: Dict[str, int],
    team: Optional[str],
    points: int,
) -> None:
    if points == 0:
        return
 
    if team == "UWRF":
        for player in lineup:
            ensure_player(plus_minus, player)
            plus_minus[player] += points
 
    elif team == "Opponent":
        for player in lineup:
            ensure_player(plus_minus, player)
            plus_minus[player] -= points
 
 
# -------------------------------------------------------------------------
# Validation
# -------------------------------------------------------------------------
 
def validate_plus_minus(
    plus_minus: Dict[str, int],
    uwrf_score: Optional[int],
    opp_score: Optional[int],
) -> None:
    """
    Check that the sum of all player plus/minus values equals
    5 * (UWRF score - opponent score).
 
    Raises ValueError if the totals don't match, so the caller knows
    something went wrong with lineup tracking or event parsing.
    """
    if uwrf_score is None or opp_score is None:
        print("WARNING: Could not parse final scores — skipping plus/minus validation.")
        return
 
    point_diff = uwrf_score - opp_score
    expected_total = 5 * point_diff
    actual_total = sum(plus_minus.values())
 
    if actual_total != expected_total:
        raise ValueError(
            f"Plus/minus validation FAILED: "
            f"sum of player values = {actual_total:+d}, "
            f"expected 5 * ({uwrf_score} - {opp_score}) = {expected_total:+d}. "
            f"Check for lineup tracking errors or missing events."
        )
 
    print(
        f"Plus/minus validation passed: "
        f"total = {actual_total:+d} = 5 * ({uwrf_score} - {opp_score})"
    )
 
 
# -------------------------------------------------------------------------
# Main computation
# -------------------------------------------------------------------------
 
def compute_game_plus_minus(game_id: int) -> Dict[str, int]:
    """
    Compute plus/minus for every UWRF player in a given game.
 
    Starting lineup is parsed automatically from the raw box score in the
    database — no manual STARTING_LINEUP_BY_GAME dict needed.
 
    The raw PDF play-by-play has two ordering quirks:
 
    1. At the same clock tick the PDF sometimes lists a UWRF sub_in *before*
       its paired sub_out (because the visitor's sub lines appear in between).
       Naively processing in event order leaves 6 players on the floor.
 
    2. At some clock ticks the sub_out for a player appears *after* scoring
       events at the same clock tick. The sub_in and sub_out straddle a score.
 
    Fix: when we see a UWRF sub_out, immediately look ahead in the same clock
    tick for the paired sub_in and execute the full swap atomically.
    """
    conn = connect_db()
 
    # --- Auto-detect starting lineup and final scores ---
    starters, uwrf_score, opp_score = get_starters_and_scores(conn, game_id)
 
    if len(starters) != 5:
        conn.close()
        raise ValueError(
            f"Expected 5 starters for game_id {game_id}, "
            f"but found {len(starters)}: {starters}. "
            f"Check that the raw box score lines were imported correctly."
        )
 
    starting_lineup = starters
    lineup: List[str] = starting_lineup.copy()
    plus_minus: Dict[str, int] = {p: 0 for p in starting_lineup}
 
    clear_existing_lineup_states(conn, game_id)
    clear_existing_player_stats(conn, game_id)
 
    events: List[list] = [list(ev) for ev in get_events_for_game(conn, game_id)]
 
    print(f"Loaded {len(events)} events for game_id {game_id}")
    print(f"Starting lineup (auto-detected): {lineup}\n")
 
    for i, ev in enumerate(events):
        event_num, period, clock, team, player, event_type, points, desc = ev
        player = normalize_player_name(player)
 
        if event_type == "sub_out" and team == "UWRF" and player:
            if player in lineup:
                lineup.remove(player)
 
            for j in range(i + 1, len(events)):
                ev2 = events[j]
                en2, p2, cl2, t2, pl2, et2, pts2, d2 = ev2
                if (p2, cl2) != (period, clock):
                    break
                pl2 = normalize_player_name(pl2)
                if t2 == "UWRF" and et2 == "sub_in" and pl2:
                    ensure_player(plus_minus, pl2)
                    if pl2 not in lineup:
                        lineup.append(pl2)
                    events[j][5] = "_sub_in_done"
                    break
 
            if len(lineup) != 5:
                raise ValueError(
                    f"Lineup size error after sub_out at event {event_num} "
                    f"(P{period} {clock}): expected 5 players, got {len(lineup)}. "
                    f"Current lineup: {lineup}"
                )
 
        elif event_type == "sub_in" and team == "UWRF" and player:
            ensure_player(plus_minus, player)
            if player not in lineup:
                lineup.append(player)
 
            if len(lineup) != 5:
                raise ValueError(
                    f"Lineup size error after sub_in at event {event_num} "
                    f"(P{period} {clock}): expected 5 players, got {len(lineup)}. "
                    f"Current lineup: {lineup}"
                )
 
        elif event_type == "_sub_in_done":
            pass
 
        elif event_type in {"made_2pt", "made_3pt", "made_ft"}:
            apply_scoring_event(
                lineup=lineup,
                plus_minus=plus_minus,
                team=team,
                points=points,
            )
 
        insert_lineup_state(
            conn=conn,
            game_id=game_id,
            event_num=event_num,
            lineup=lineup.copy(),
        )
 
    insert_player_game_stats(conn, game_id, plus_minus)
    conn.commit()
    conn.close()
 
    # --- Validate total plus/minus equals 5 * point differential ---
    validate_plus_minus(plus_minus, uwrf_score, opp_score)
 
    return plus_minus
 
 
def print_plus_minus_results(plus_minus: Dict[str, int]) -> None:
    print("\nFinal plus/minus:")
    for player, value in sorted(plus_minus.items(), key=lambda x: x[1], reverse=True):
        print(f"{player}: {value:+d}")
 
 
# -------------------------------------------------------------------------
# LOOP THROUGH ALL GAMES
# -------------------------------------------------------------------------
def compute_all_games_plus_minus():
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute("SELECT game_id FROM games ORDER BY game_id")
    game_ids = [row[0] for row in cursor.fetchall()]

    conn.close()

    print(f"Found {len(game_ids)} games.\n")

    for game_id in game_ids:
        print(f"==============================")
        print(f"Processing game_id {game_id}")
        print(f"==============================")

        try:
            results = compute_game_plus_minus(game_id)
            print_plus_minus_results(results)
            print("\n")

        except Exception as e:
            print(f"ERROR in game {game_id}: {e}\n")


# -------------------------------------------------------------------------
# RUN
# -------------------------------------------------------------------------
if __name__ == "__main__":
    compute_all_games_plus_minus()

 
