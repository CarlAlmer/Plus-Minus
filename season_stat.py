import sqlite3
from typing import Dict, Any, List

DB_PATH = "basketball.db"


# --------------------------------------------------
# DATABASE
# --------------------------------------------------
def connect_db() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def safe_int(value) -> int:
    if value is None:
        return 0
    return int(value)


def safe_float(value) -> float:
    if value is None:
        return 0.0
    return float(value)


def safe_pct(makes: int, attempts: int) -> float:
    if attempts == 0:
        return 0.0
    return round(makes / attempts, 4)


def per_40(total: float, minutes: float) -> float:
    if minutes == 0:
        return 0.0
    return round((total / minutes) * 40, 4)


# --------------------------------------------------
# CLEAR OLD SEASON TABLES
# --------------------------------------------------
def clear_season_tables(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    cursor.execute("DELETE FROM season_plus_minus_stats")
    cursor.execute("DELETE FROM player_season_stats")
    conn.commit()


# --------------------------------------------------
# LOAD ALL GAME-LEVEL PLAYER STATS
# --------------------------------------------------
def get_all_player_game_stats(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            game_id,
            player_name,
            plus_minus,
            min_played,
            stints,
            fg_makes,
            fg_attempts,
            pt3_makes,
            pt3_attempts,
            ft_makes,
            ft_attempts,
            points,
            assists,
            orb,
            drb,
            fouls,
            turnovers,
            blocks,
            steals
        FROM player_game_stats
        WHERE player_name IS NOT NULL
        ORDER BY player_name, game_id
        """
    )

    return cursor.fetchall()


# --------------------------------------------------
# AGGREGATE SEASON TOTALS
# --------------------------------------------------
def aggregate_season_stats(rows: List[sqlite3.Row]) -> Dict[str, Dict[str, Any]]:
    season_stats: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        player_name = row["player_name"]

        if player_name not in season_stats:
            season_stats[player_name] = {
                "player_name": player_name,
                "game_ids": set(),
                "plus_minus": 0,
                "min_played": 0.0,
                "stints": 0,
                "fg_makes": 0,
                "fg_attempts": 0,
                "pt3_makes": 0,
                "pt3_attempts": 0,
                "ft_makes": 0,
                "ft_attempts": 0,
                "points": 0,
                "assists": 0,
                "orb": 0,
                "drb": 0,
                "rebounds": 0,
                "fouls": 0,
                "turnovers": 0,
                "blocks": 0,
                "steals": 0,
            }

        p = season_stats[player_name]

        p["game_ids"].add(safe_int(row["game_id"]))
        p["plus_minus"] += safe_int(row["plus_minus"])
        p["min_played"] += safe_float(row["min_played"])
        p["stints"] += safe_int(row["stints"])

        p["fg_makes"] += safe_int(row["fg_makes"])
        p["fg_attempts"] += safe_int(row["fg_attempts"])

        p["pt3_makes"] += safe_int(row["pt3_makes"])
        p["pt3_attempts"] += safe_int(row["pt3_attempts"])

        p["ft_makes"] += safe_int(row["ft_makes"])
        p["ft_attempts"] += safe_int(row["ft_attempts"])

        p["points"] += safe_int(row["points"])
        p["assists"] += safe_int(row["assists"])

        orb = safe_int(row["orb"])
        drb = safe_int(row["drb"])
        p["orb"] += orb
        p["drb"] += drb
        p["rebounds"] += (orb + drb)

        p["fouls"] += safe_int(row["fouls"])
        p["turnovers"] += safe_int(row["turnovers"])
        p["blocks"] += safe_int(row["blocks"])
        p["steals"] += safe_int(row["steals"])

    # Derived season stats
    for player_name, p in season_stats.items():
        p["games_played"] = len(p["game_ids"])

        p["fg_percentage"] = safe_pct(p["fg_makes"], p["fg_attempts"])
        p["pt3_percentage"] = safe_pct(p["pt3_makes"], p["pt3_attempts"])
        p["ft_percentage"] = safe_pct(p["ft_makes"], p["ft_attempts"])

        p["plus_minus_per_40"] = per_40(p["plus_minus"], p["min_played"])
        p["points_per_40"] = per_40(p["points"], p["min_played"])
        p["assists_per_40"] = per_40(p["assists"], p["min_played"])

        if p["stints"] > 0:
            p["plus_minus_per_stint"] = round(p["plus_minus"] / p["stints"], 4)
        else:
            p["plus_minus_per_stint"] = 0.0

    return season_stats


# --------------------------------------------------
# INSERT INTO season_plus_minus_stats
# --------------------------------------------------
def insert_season_plus_minus_stats(
    conn: sqlite3.Connection,
    season_stats: Dict[str, Dict[str, Any]]
) -> None:
    cursor = conn.cursor()

    for player_name in sorted(season_stats.keys()):
        p = season_stats[player_name]

        cursor.execute(
            """
            INSERT INTO season_plus_minus_stats (
                player_name,
                games_played,
                plus_minus,
                min_played,
                plus_minus_per_40,
                stints,
                plus_minus_per_stint,
                points,
                points_per_40,
                assists,
                assists_per_40
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                p["player_name"],
                p["games_played"],
                p["plus_minus"],
                round(p["min_played"], 4),
                p["plus_minus_per_40"],
                p["stints"],
                p["plus_minus_per_stint"],
                p["points"],
                p["points_per_40"],
                p["assists"],
                p["assists_per_40"],
            ),
        )

    conn.commit()


# --------------------------------------------------
# INSERT INTO player_season_stats
# --------------------------------------------------
def insert_player_season_stats(
    conn: sqlite3.Connection,
    season_stats: Dict[str, Dict[str, Any]]
) -> None:
    cursor = conn.cursor()

    for player_name in sorted(season_stats.keys()):
        p = season_stats[player_name]

        cursor.execute(
            """
            INSERT INTO player_season_stats (
                player_name,
                games_played,
                fg_makes,
                fg_attempts,
                fg_percentage,
                pt3_makes,
                pt3_attempts,
                pt3_percentage,
                ft_makes,
                ft_attempts,
                ft_percentage,
                rebounds,
                fouls,
                turnovers,
                blocks,
                steals
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                p["player_name"],
                p["games_played"],
                p["fg_makes"],
                p["fg_attempts"],
                p["fg_percentage"],
                p["pt3_makes"],
                p["pt3_attempts"],
                p["pt3_percentage"],
                p["ft_makes"],
                p["ft_attempts"],
                p["ft_percentage"],
                p["rebounds"],
                p["fouls"],
                p["turnovers"],
                p["blocks"],
                p["steals"],
            ),
        )

    conn.commit()


# --------------------------------------------------
# PRINT RESULTS
# --------------------------------------------------
def print_summary(season_stats: Dict[str, Dict[str, Any]]) -> None:
    print("\nSeason totals computed:\n")

    for player_name in sorted(season_stats.keys()):
        p = season_stats[player_name]
        print(
            f"{p['player_name']}: "
            f"GP={p['games_played']}, "
            f"+/-={p['plus_minus']:+d}, "
            f"MIN={p['min_played']:.1f}, "
            f"+/-/40={p['plus_minus_per_40']:.4f}, "
            f"STINTS={p['stints']}, "
            f"+/-/STINT={p['plus_minus_per_stint']:.4f}, "
            f"PTS={p['points']}, "
            f"PTS/40={p['points_per_40']:.4f}, "
            f"AST={p['assists']}, "
            f"AST/40={p['assists_per_40']:.4f}, "
            f"REB={p['rebounds']}, "
            f"FG%={p['fg_percentage']:.4f}, "
            f"3PT%={p['pt3_percentage']:.4f}, "
            f"FT%={p['ft_percentage']:.4f}"
        )


# --------------------------------------------------
# MAIN RUNNER
# --------------------------------------------------
def compute_season_stats() -> None:
    conn = connect_db()

    try:
        rows = get_all_player_game_stats(conn)

        if not rows:
            print("No rows found in player_game_stats. Nothing to aggregate.")
            return

        print(f"Loaded {len(rows)} player-game stat rows.")

        clear_season_tables(conn)
        print("Cleared old season tables.")

        season_stats = aggregate_season_stats(rows)

        insert_season_plus_minus_stats(conn, season_stats)
        insert_player_season_stats(conn, season_stats)

        print_summary(season_stats)
        print("\nSeason stats tables updated successfully.")

    finally:
        conn.close()


# --------------------------------------------------
# RUN
# --------------------------------------------------
if __name__ == "__main__":
    compute_season_stats()