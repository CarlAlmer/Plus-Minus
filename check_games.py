"""Check every game in the database for stats that don't add up.
 
Run from the project folder:
    python scripts\\check_games.py            (men's database)
    python scripts\\check_games.py --db data\\basketball_women.db
 
For each game it checks that (1) the players' plus/minus adds up to 5 x the final
margin, and (2) UWRF has plays in the play-by-play. Games with no play-by-play
in the PDF are listed separately (they are box-score only by design)."""
import argparse, os, sqlite3, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from compute_plus_minus import parse_starters_and_scores
 
ap = argparse.ArgumentParser()
ap.add_argument("--export", action="store_true",
                help="also save each flagged game's text into data\\flagged_games\\ so it can be sent to me")
ap.add_argument("--db", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "basketball.db"))
args = ap.parse_args()
db = args.db
conn = sqlite3.connect(db)
games = conn.execute("SELECT game_id, game_date, opponent FROM games ORDER BY game_date").fetchall()
bad, minor, nopbp = [], [], []
for gid, d, opp in games:
    lines = [r[0] for r in conn.execute("SELECT line_text FROM raw_lines WHERE game_id=? ORDER BY line_id", (gid,))]
    if not any("play by play" in l.lower() for l in lines):
        nopbp.append(f"{d}  {opp}")
        continue
    _, us, them = parse_starters_and_scores(lines)
    pm = conn.execute("SELECT SUM(plus_minus), COUNT(*) FROM player_game_stats WHERE game_id=?", (gid,)).fetchone()
    uw = conn.execute("SELECT COUNT(*) FROM events WHERE game_id=? AND team='UWRF'", (gid,)).fetchone()[0]
    problems = []
    if us and them and pm[0] != 5 * (us - them):
        problems.append(f"plus/minus adds to {pm[0]}, expected {5 * (us - them):+d}")
    if uw == 0:
        problems.append("no UWRF plays found (player names probably didn't match)")
    if problems and pm[0] is not None and uw and abs(pm[0] - 5 * (us - them)) <= 10 and len(problems) == 1:
        # only a few points off and every play was matched to a player: the source play-by-play's
        # own substitutions don't line up (e.g. a sub listed out of order) - minor
        minor.append(f"{d}  {opp}: plus/minus adds to {pm[0]}, expected {5 * (us - them):+d}")
        problems = []
    if problems:
        # names the play-by-play credits to the OPPONENT that share a last name with a UWRF player
        # in this game's box score: the likely cause of a name that didn't match
        ours = [r[0] for r in conn.execute("SELECT player_name FROM player_game_stats WHERE game_id=?", (gid,))]
        last = {n.split(",")[0] for n in ours if n}
        theirs = {r[0] for r in conn.execute(
            "SELECT DISTINCT player FROM events WHERE game_id=? AND team='Opponent' AND player IS NOT NULL", (gid,))}
        sus = sorted(t for t in theirs if t.split(",")[0] in last)
        if sus:
            problems.append("play-by-play names not matched to our players: " + ", ".join(sus))
        bad.append(f"{d}  {opp}: " + "; ".join(problems))
        if args.export:
            folder = os.path.join(os.path.dirname(os.path.abspath(db)), "flagged_games")
            os.makedirs(folder, exist_ok=True)
            with open(os.path.join(folder, f"{d}_{opp}.txt"), "w", encoding="utf-8") as f:
                f.write("\n".join(lines))
print(f"Checked {len(games)} games.")
print(f"\nBox score only (no play-by-play in the PDF): {len(nopbp)}")
for x in nopbp: print("  " + x)
print(f"\nMinor (within 10 points - the PDF's own substitutions don't line up exactly): {len(minor)}")
for x in minor: print("  " + x)
print(f"\nGames with stats that don't add up: {len(bad)}")
for x in bad: print("  " + x)
if not bad: print("  none - everything checks out.")