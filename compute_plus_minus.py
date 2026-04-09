import sqlite3
from typing import Dict, List, Optional, Tuple
 
 
DB_PATH = "data/basketball.db"
 
STARTING_LINEUP_BY_GAME = {
    3: [
        "HAVLIK,OWEN",
        "THOMPSON,MICAH",
        "LEIFKER,JACK",
        "RALPH,GAVIN",
        "WANGUHU,JEREMY",
    ]
}
 
 
def connect_db() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)
 
 
def normalize_player_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    import re
    return re.sub(r",\s+", ",", name.strip().upper())
 
 
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
 
    # Check if this combination already exists
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
 
    # New combination — insert it
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
 
 
def compute_game_plus_minus(game_id: int, preview_scoring: bool = True) -> Dict[str, int]:
    """
    Compute plus/minus for every UWRF player in a given game.
 
    The raw PDF play-by-play has two ordering quirks that the original
    clock-grouping logic didn't handle correctly:
 
    1. At the same clock tick the PDF sometimes lists a UWRF sub_in *before*
       its paired sub_out (because the visitor's sub lines appear in between).
       Naively processing in event order leaves 6 players on the floor.
 
    2. At 15:13 in the first half (and similar spots), the sub_out for a player
       appears *after* the scoring events at the same clock tick.  The sub_in
       and sub_out for the same substitution straddle a scoring event.
 
    Fix: process events in order, but when we see a UWRF sub_out, immediately
    look ahead in the same clock tick for the paired sub_in and execute the
    full swap atomically.  This keeps the lineup at exactly 5 at every scoring
    event without ever buffering or reordering scoring events.
    """
    if game_id not in STARTING_LINEUP_BY_GAME:
        raise ValueError(
            f"No starting lineup found for game_id {game_id}. "
            f"Add it to STARTING_LINEUP_BY_GAME first."
        )
 
    starting_lineup = [normalize_player_name(p) for p in STARTING_LINEUP_BY_GAME[game_id]]
    lineup: List[str] = starting_lineup.copy()
 
    plus_minus: Dict[str, int] = {p: 0 for p in starting_lineup}
 
    conn = connect_db()
    clear_existing_lineup_states(conn, game_id)
    clear_existing_player_stats(conn, game_id)
 
    # Work on a mutable copy so we can mark sub_in events as already processed
    events: List[list] = [list(ev) for ev in get_events_for_game(conn, game_id)]
 
    print(f"Loaded {len(events)} events for game_id {game_id}")
    print(f"Starting lineup: {lineup}\n")
 
    for i, ev in enumerate(events):
        event_num, period, clock, team, player, event_type, points, desc = ev
        player = normalize_player_name(player)
 
        if event_type == "sub_out" and team == "UWRF" and player:
            # Remove the outgoing player immediately.
            if player in lineup:
                lineup.remove(player)
 
            # Look ahead in the same clock tick for the matching sub_in and
            # apply it now, so the lineup stays at 5 even when the PDF lists
            # the sub_in later (possibly after scoring events).
            for j in range(i + 1, len(events)):
                ev2 = events[j]
                en2, p2, cl2, t2, pl2, et2, pts2, d2 = ev2
                if (p2, cl2) != (period, clock):
                    break  # Left this clock tick — no more candidates
                pl2 = normalize_player_name(pl2)
                if t2 == "UWRF" and et2 == "sub_in" and pl2:
                    ensure_player(plus_minus, pl2)
                    if pl2 not in lineup:
                        lineup.append(pl2)
                    # Mark as handled so the forward pass skips it
                    events[j][5] = "_sub_in_done"
                    break
 
        elif event_type == "sub_in" and team == "UWRF" and player:
            # Orphan sub_in (no preceding sub_out at this clock tick).
            # This happens at the start of each half when multiple players
            # enter simultaneously.  Just add the player.
            ensure_player(plus_minus, player)
            if player not in lineup:
                lineup.append(player)
 
        elif event_type == "_sub_in_done":
            pass  # Already handled by the look-ahead above
 
        elif event_type in {"made_2pt", "made_3pt", "made_ft"}:
            if preview_scoring:
                print(
                    f"Event {event_num} | P{period} {clock} | {team} {event_type} {points}"
                    f" | lineup={lineup}"
                )
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
 
    return plus_minus
 
 
def print_plus_minus_results(plus_minus: Dict[str, int]) -> None:
    print("\nFinal plus/minus:")
    for player, value in sorted(plus_minus.items(), key=lambda x: x[1], reverse=True):
        print(f"{player}: {value:+d}")
 
 
if __name__ == "__main__":
    game_id = 3
    results = compute_game_plus_minus(game_id, preview_scoring=True)
    print_plus_minus_results(results)
 
