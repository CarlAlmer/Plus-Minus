import sqlite3
import re
from typing import Dict, List, Optional, Tuple

DB_PATH = "basketball.db"


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


def warn(message: str) -> None:
    print(f"WARNING: {message}")


# -------------------------------------------------------------------------
# STARTERS FROM RAW BOX SCORE
# -------------------------------------------------------------------------
def parse_starters(lines: List[str]) -> List[str]:
    """
    Pull the 5 UWRF starters from the raw box score section.
    Starters are marked with * in the UWRF box score.
    """
    starters = []
    in_uwrf_box = False

    for line in lines:
        clean = line.strip()

        # UWRF header in the raw lines
        if re.match(r"^UW-River Falls\s+\d+$", clean):
            in_uwrf_box = True
            continue

        if in_uwrf_box:
            if clean.startswith("Totals"):
                break

            match = re.match(
                r"^\d+\s+([\w,\-\'\.]+(?:\s+[\w\-\'\.]+)?)\s+\*",
                clean
            )
            if match:
                starters.append(normalize_player_name(match.group(1).strip()))

    return starters


def get_starters(conn: sqlite3.Connection, game_id: int) -> List[str]:
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
    lines = [row[0] for row in cursor.fetchall()]
    return parse_starters(lines)


# -------------------------------------------------------------------------
# DB HELPERS
# -------------------------------------------------------------------------
def get_all_game_ids(conn: sqlite3.Connection) -> List[int]:
    cursor = conn.cursor()
    cursor.execute("SELECT game_id FROM games ORDER BY game_id")
    return [row[0] for row in cursor.fetchall()]


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


def upsert_player_stints(
    conn: sqlite3.Connection,
    game_id: int,
    stints: Dict[str, int]
) -> None:
    """
    Only updates/inserts the stints column.
    Does not overwrite plus_minus or box score stats.
    """
    cursor = conn.cursor()

    for player, stint_count in sorted(stints.items()):
        cursor.execute(
            """
            SELECT stat_id
            FROM player_game_stats
            WHERE game_id = ? AND player_name = ?
            """,
            (game_id, player),
        )
        row = cursor.fetchone()

        if row:
            cursor.execute(
                """
                UPDATE player_game_stats
                SET stints = ?
                WHERE game_id = ? AND player_name = ?
                """,
                (stint_count, game_id, player),
            )
        else:
            cursor.execute(
                """
                INSERT INTO player_game_stats (game_id, player_name, stints)
                VALUES (?, ?, ?)
                """,
                (game_id, player, stint_count),
            )

    conn.commit()


# -------------------------------------------------------------------------
# STINT HELPERS
# -------------------------------------------------------------------------
def ensure_player(stints: Dict[str, int], active_stints: Dict[str, bool], player: Optional[str]) -> None:
    player = normalize_player_name(player)
    if not player:
        return

    if player not in stints:
        stints[player] = 0
    if player not in active_stints:
        active_stints[player] = False


def start_stint(
    stints: Dict[str, int],
    active_stints: Dict[str, bool],
    player: Optional[str]
) -> None:
    player = normalize_player_name(player)
    if not player:
        return

    ensure_player(stints, active_stints, player)

    if not active_stints[player]:
        stints[player] += 1
        active_stints[player] = True


def end_stint(
    active_stints: Dict[str, bool],
    player: Optional[str]
) -> None:
    player = normalize_player_name(player)
    if not player:
        return

    if active_stints.get(player, False):
        active_stints[player] = False


def start_lineup_stints(
    lineup: List[str],
    stints: Dict[str, int],
    active_stints: Dict[str, bool]
) -> None:
    for player in lineup:
        start_stint(stints, active_stints, player)


def end_lineup_stints(
    lineup: List[str],
    active_stints: Dict[str, bool]
) -> None:
    for player in lineup:
        end_stint(active_stints, player)


# -------------------------------------------------------------------------
# MAIN STINT COMPUTATION
# -------------------------------------------------------------------------
def compute_game_stints(game_id: int) -> Dict[str, int]:
    """
    Stint rules:
      - starters begin with a stint
      - sub_in starts a new stint
      - sub_out ends a stint
      - changing periods ends all current stints and starts new ones for the
        players who begin the new period on the floor
    """
    conn = connect_db()

    try:
        starters = get_starters(conn, game_id)
        if len(starters) != 5:
            raise ValueError(
                f"Expected 5 starters for game_id {game_id}, found {len(starters)}: {starters}"
            )

        events = [list(ev) for ev in get_events_for_game(conn, game_id)]

        print(f"Loaded {len(events)} events for game_id {game_id}")
        print(f"Starting lineup: {starters}")

        lineup: List[str] = starters.copy()
        stints: Dict[str, int] = {}
        active_stints: Dict[str, bool] = {}

        # starters begin the game with stint 1
        start_lineup_stints(lineup, stints, active_stints)

        previous_period = None

        for i, ev in enumerate(events):
            event_num, period, clock, team, player, event_type, points, description = ev
            player = normalize_player_name(player)

            # halftime / new period split
            if previous_period is not None and period != previous_period:
                end_lineup_stints(lineup, active_stints)
                start_lineup_stints(lineup, stints, active_stints)

            previous_period = period

            # only track UWRF players
            if team != "UWRF":
                continue

            if event_type == "sub_out" and player:
                end_stint(active_stints, player)

                if player in lineup:
                    lineup.remove(player)
                else:
                    warn(
                        f"sub_out player {player} was not in lineup at "
                        f"event {event_num} (P{period} {clock})"
                    )

                paired_sub_in_found = False

                # find matching same-clock sub_in
                for j in range(i + 1, len(events)):
                    ev2 = events[j]
                    en2, p2, cl2, t2, pl2, et2, pts2, desc2 = ev2

                    if (p2, cl2) != (period, clock):
                        break

                    pl2 = normalize_player_name(pl2)

                    if t2 == "UWRF" and et2 == "sub_in" and pl2:
                        ensure_player(stints, active_stints, pl2)

                        if pl2 not in lineup:
                            lineup.append(pl2)
                            start_stint(stints, active_stints, pl2)
                        else:
                            warn(
                                f"sub_in player {pl2} already in lineup at "
                                f"event {en2} (P{p2} {cl2})"
                            )

                        events[j][5] = "_sub_in_done"
                        paired_sub_in_found = True
                        break

                if not paired_sub_in_found:
                    warn(
                        f"No matching same-clock sub_in found after sub_out for {player} "
                        f"at event {event_num} (P{period} {clock})"
                    )

                if len(lineup) != 5:
                    warn(
                        f"Lineup size issue after sub_out at event {event_num} "
                        f"(P{period} {clock}): expected 5, got {len(lineup)}. "
                        f"Current lineup: {lineup}"
                    )

            elif event_type == "sub_in" and player:
                ensure_player(stints, active_stints, player)

                if player not in lineup:
                    lineup.append(player)
                    start_stint(stints, active_stints, player)
                else:
                    warn(
                        f"sub_in player {player} already in lineup at "
                        f"event {event_num} (P{period} {clock})"
                    )

                if len(lineup) != 5:
                    warn(
                        f"Lineup size issue after sub_in at event {event_num} "
                        f"(P{period} {clock}): expected 5, got {len(lineup)}. "
                        f"Current lineup: {lineup}"
                    )

            elif event_type == "_sub_in_done":
                pass

        # close everyone at end of game
        end_lineup_stints(lineup, active_stints)

        upsert_player_stints(conn, game_id, stints)
        return stints

    finally:
        conn.close()


def print_stint_results(stints: Dict[str, int]) -> None:
    print("\nStints:")
    for player, value in sorted(stints.items(), key=lambda x: x[1], reverse=True):
        print(f"{player}: {value}")


# -------------------------------------------------------------------------
# RUN FOR ALL GAMES
# -------------------------------------------------------------------------
def compute_all_games_stints() -> None:
    conn = connect_db()
    game_ids = get_all_game_ids(conn)
    conn.close()

    print(f"Found {len(game_ids)} games.\n")

    for game_id in game_ids:
        print("==============================")
        print(f"Processing game_id {game_id}")
        print("==============================")

        try:
            stints = compute_game_stints(game_id)
            print_stint_results(stints)
            print()
        except Exception as e:
            print(f"ERROR in game {game_id}: {e}\n")


# -------------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------------
if __name__ == "__main__":
    # Single game:
    # stints = compute_game_stints(11)
    # print_stint_results(stints)

    # All games:
    compute_all_games_stints()