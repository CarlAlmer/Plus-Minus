
# run_all.py

import os
import sqlite3
import runpy

# Make sure all files use the same basketball.db
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE_DIR)

DB_PATH = os.path.join(BASE_DIR, "basketball.db")


def count_rows(table_name):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    conn.close()
    return count


def get_all_game_ids():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT game_id FROM games ORDER BY game_id")
    game_ids = [row[0] for row in cursor.fetchall()]

    conn.close()
    return game_ids


def show_counts(step_name):
    print(f"\nDatabase check after {step_name}:")
    for table in ["games", "raw_lines", "events", "lineups", "lineup_states", "player_game_stats"]:
        try:
            print(f"  {table}: {count_rows(table)} rows")
        except Exception:
            print(f"  {table}: table not found")


def parse_events_for_all_games():
    from parse_events import parse_game_events

    game_ids = get_all_game_ids()

    print(f"\nFound {len(game_ids)} games to parse events for.\n")

    for game_id in game_ids:
        print("==============================")
        print(f"Parsing events for game_id {game_id}")
        print("==============================")

        try:
            parse_game_events(
                game_id=game_id,
                delete_existing=True
            )
        except Exception as e:
            print(f"ERROR parsing events for game {game_id}: {e}\n")


def run_all():
    print(f"Running from folder: {BASE_DIR}")
    print(f"Using database: {DB_PATH}")

    print("\nSTEP 1: Creating database/tables")
    print("--------------------------------")
    runpy.run_path(os.path.join(BASE_DIR, "create_database.py"))
    show_counts("creating database")

    print("\nSTEP 2: Importing games")
    print("-----------------------")
    from bulk_import import run_bulk_import
    run_bulk_import()
    show_counts("bulk import")

    print("\nSTEP 3: Parsing play-by-play events")
    print("-----------------------------------")
    parse_events_for_all_games()
    show_counts("parsing events")

    if count_rows("events") == 0:
        print("\nSTOPPED: events table is still empty.")
        print("This means raw_lines were imported, but parse_events.py did not recognize the play-by-play section.")
        return

    print("\nSTEP 4: Computing plus/minus")
    print("---------------------------")
    from compute_plus_minus import compute_all_games_plus_minus
    compute_all_games_plus_minus()
    show_counts("plus/minus")

    print("\nSTEP 5: Parsing box score stats")
    print("-------------------------------")
    from box_score_stats import parse_all_games
    parse_all_games()
    show_counts("box score stats")

    print("\nSTEP 6: Computing stints")
    print("------------------------")
    from stints import compute_all_games_stints
    compute_all_games_stints()
    show_counts("stints")

    print("\nSTEP 7: Computing season stats")
    print("------------------------------")
    from season_stat import compute_season_stats
    compute_season_stats()

    print("\nALL DONE. Database is fully updated.")


if __name__ == "__main__":
    run_all()