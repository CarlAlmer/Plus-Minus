import sqlite3

# this path could be different things depending on where you code is stored on your computer
# such as "data/basketball.db"
DB_PATH = "basketball.db"


def clear_all_tables():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("Clearing all tables...\n")

    # Disable foreign key checks temporarily (important if you add constraints later)
    cursor.execute("PRAGMA foreign_keys = OFF;")

    tables = [
        "events",
        "lineup_states",
        "player_game_stats",
        "raw_lines",
        "games"
    ]

    for table in tables:
        print(f"Clearing table: {table}")
        cursor.execute(f"DELETE FROM {table};")

    # Reset auto-increment counters
    print("\nResetting auto-increment counters...")
    cursor.execute("DELETE FROM sqlite_sequence;")

    # Re-enable foreign keys
    cursor.execute("PRAGMA foreign_keys = ON;")

    conn.commit()
    conn.close()

    print("\n✅ Database cleaned successfully.")


if __name__ == "__main__":
    clear_all_tables()
