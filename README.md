# Plus-Minus

Plus/minus analytics for UW-River Falls basketball, available to anyone through a web app, with nothing to install.

The project reads each game's box score from uwrfsports.com, works out who was on the floor for every play, and calculates plus/minus for every player. The results are shown in a password-protected Streamlit dashboard. It began as an URSCA-funded UWRF student project, and feedback from the men's basketball coaching staff shaped the dashboard (lineup views, per-stint printouts, a password so opponents can't see it, and recent-games views).

## What it does

- **Reads the box scores.** Each game's PDF is downloaded from the team's schedule page and turned into text.
- **Builds rosters automatically.** Each season's players come from the box scores, so no list has to be kept by hand.
- **Tracks lineups.** Starters come from the box score, and every substitution in the play-by-play moves players on and off the floor, including at the start of the second half and overtime.
- **Calculates plus/minus, stints, lineup results and season totals** for every player.
- **Checks itself.** For every game, all players' plus/minus must add up to 5 x the final margin. Games that don't match are flagged, never hidden.
- **Handles older seasons.** Men's data goes back to 2019-20, and the code copes with the different name formats and layouts used in older PDFs.
- **Keeps itself up to date.** The app finds new games, imports them, and rebuilds the stats with one button.

## How it works

1. **Find games.** The app looks at the team's schedule page and finds finished games it doesn't have yet. Exhibitions are skipped, and games played today wait for the next check.
2. **Read the box score.** It opens each game's box score page, finds the PDF link, downloads the PDF, and stores the text.
3. **Build the roster.** Player names come from the box score tables, and the play-by-play names are matched to them.
4. **Parse the play-by-play.** Every line (shots, rebounds, fouls, turnovers, substitutions) becomes an event with a period, a clock time, and a score.
5. **Track the lineup.** Starting with the five starters, each substitution changes who is on the floor. Every point scored while a player is on the floor counts toward their plus/minus, for or against.
6. **Calculate the stats.** This produces plus/minus, stints, per-lineup results, per-game box score stats, and season totals including per-40 numbers.
7. **Check the numbers.** The plus/minus total for each game is compared against 5 x the final margin. Games within 10 points are marked "Minor" (usually an error in the source PDF's substitution log), and larger gaps are flagged for review. Games with no play-by-play are marked "box score only", and their stats are kept without plus/minus.
8. **Show it.** The dashboard has a team picker, a season dropdown, and views for players and games.

## Using the app

Open the app's link and log in with the team password. Men's and women's each have their own password and their own database.

Pressing **Check for new games** looks at the current season's schedule, imports anything new, updates the stats, and saves the database back to GitHub so nothing is lost when the app restarts. After a save the app restarts, so log in again.

The app is hosted on Streamlit Community Cloud and deploys from the `main` branch of this repo. Passwords and the GitHub token are kept in Streamlit's **Secrets** settings, never in the repo. The token is a GitHub personal access token with the `repo` scope, and it expires, so check it first if the button ever reports a GitHub error.

## Files

| File | What it does |
|---|---|
| `dashboard.py` | The Streamlit app |
| `new_games.py` | Finds new games on the site, imports them, and runs the full pipeline |
| `github_sync.py` | Saves the database to GitHub for the hosted app |
| `import_game.py` | Downloads one game's PDF and stores its text |
| `bulk_import.py` | Imports many games at once |
| `roster.py` | Builds each season's roster from the box scores |
| `parse_events.py` | Turns play-by-play text into events (shots, rebounds, subs) |
| `compute_plus_minus.py` | Tracks the lineup and calculates plus/minus |
| `stints.py` | Counts each player's stints |
| `box_score_stats.py` | Per-game box score stats, including games with no play-by-play |
| `season_stat.py` | Season totals and per-40 numbers |
| `check_games.py` | Reports games whose numbers don't add up |
| `repair_games.py` | Re-processes every game in a database from scratch |
| `run_all.py` | Re-runs the full pipeline on a database |
| `create_database.py` | Creates the database tables |
| `cleaning.py` | Cleans up data (names and formats) |
| `fix_db.py` | One-off database fixes |
| `delete_game.py` | Removes a game from the database |
| `list_pdfs.py` | Lists each game's box score page and PDF link for a season |
| `box_ids.py` | Prints each game's box score ID, for spotting URL patterns |
| `test_new_games.py`, `test_dashboard.py` | Automated tests |
| `requirements.txt` | Python packages the app needs |
| `basketball.db` | The men's database (SQLite). The women's is `basketball_women.db` |

## Built with

Python, SQLite, Streamlit, pandas, pdfplumber, the GitHub API

## Team

Built by UW-River Falls students Albert Blair, Carl Almer, and Jeremy Sherlock, advised by Dr. Loper, with URSCA grant funding, for the UWRF basketball programs.
