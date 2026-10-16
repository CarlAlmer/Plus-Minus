"""Re-process every game already in the database with the current name fixes.
 
Games loaded before the fixes (names like 'Logan Jedwabny' or 'Reeck ,Jake') can have
wrong plus/minus without any error. This rewrites the saved box score text of every
game into the standard 'Last,First' style, then rebuilds events, plus/minus, stints
and season totals for ALL games. Safe to run more than once.
 
Run from the project folder:
    python scripts\\repair_games.py                      (men's database)
    python scripts\\repair_games.py --db data\\basketball_women.db
Then run  python scripts\\check_games.py  to see what is still off."""
import argparse, os, shutil, sqlite3, sys
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import new_games
 
ap = argparse.ArgumentParser()
ap.add_argument("--db", default=os.path.join(HERE, "..", "data", "basketball.db"))
db = os.path.abspath(ap.parse_args().db)
 
backup = db + ".before_repair"
if not os.path.exists(backup):
    shutil.copy(db, backup)
    print(f"Saved a backup copy: {backup}")
 
conn = sqlite3.connect(db)
game_ids = [r[0] for r in conn.execute("SELECT game_id FROM games ORDER BY game_id")]
changed = 0
for gid in game_ids:
    rows = conn.execute("SELECT line_id, line_text FROM raw_lines WHERE game_id=? ORDER BY line_id", (gid,)).fetchall()
    old = [r[1] for r in rows]
    new = new_games.normalize_player_names(new_games.normalize_school_name(old))
    n = 0
    for (line_id, _), a, b in zip(rows, old, new):
        if a != b:
            conn.execute("UPDATE raw_lines SET line_text=? WHERE line_id=?", (b, line_id))
            n += 1
    if n:
        changed += 1
# the roster is rebuilt from the box scores below; clear it so old spellings don't linger
try:
    conn.execute("DELETE FROM roster_appearances")
except sqlite3.OperationalError:
    pass        # table not created yet (older database): the pipeline creates it
conn.commit()
conn.close()
print(f"Rewrote names in {changed} of {len(game_ids)} games. Rebuilding stats for all games (this takes a minute)...")
 
new_games.run_pipeline(db, game_ids)
print("Done. Now run:  python scripts\\check_games.py")