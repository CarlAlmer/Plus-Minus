
"""
new_games.py
------------
Checks uwrfsports.com for finished games that are not in the database yet,
imports them, and rebuilds the stats. Used by the dashboard's
"Check for new games" button, and also runnable from a terminal:
 
    python scripts\\new_games.py mens
    python scripts\\new_games.py mens --season 2025-26     (re-check an old season)
 
HOW IT FINDS GAMES (all of this was read off last season's real pages)
  1. The season schedule page  https://uwrfsports.com/sports/mens-basketball/schedule/2026-27
     links every played game's box score page:
         /sports/mens-basketball/stats/2026-27/<opponent>/boxscore/<id>
  2. Each box score page has a "View PDF" link - the same play-by-play PDF the
     rest of this project already reads.
  3. The date, opponent and home/away are read from the first lines of that
     PDF ("Team A (rec) -vs- Team B (rec)" then "MM/DD/YY at <site>"). The
     team listed second is the home team, which matches how the existing
     games in the database were labelled.
 
THINGS IT DELIBERATELY DOES NOT DO
  * Exhibition games are skipped (the existing 25-game season has none).
  * A game dated today is left for the next check, so a game still being
    played is never imported half-finished and then never refreshed.
  * A game whose date is already in the database is never imported twice.
 
MEN'S AND WOMEN'S
  Each team has its own database file (basketball.db / basketball_women.db)
  so the two never mix. The women's file is created automatically the first
  time it is needed.
"""
 
import contextlib
import importlib
import io
import os
import re
import sqlite3
import threading
from datetime import date, datetime
from typing import Dict, List, Optional
from urllib.parse import quote, unquote, urljoin
 
import requests
 
import roster
 
HERE = os.path.dirname(os.path.abspath(__file__))
SITE = "https://uwrfsports.com"
# Look like an ordinary browser: some sites stall or ignore requests from scripts.
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
REQUEST_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/pdf,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
# (seconds to connect, seconds to wait for the reply) - so a stuck page fails
# quickly instead of hanging, and is retried.
REQUEST_TIMEOUT = (10, 25)
REQUEST_TRIES = 3
 
# The command line sets this to print() so you can watch progress; the
# dashboard leaves it unset.
progress = None
last_pdf_source = None      # address the most recent PDF really came from (for --probe)
 
 
def _say(text: str) -> None:
    if progress:
        progress(text)
 
TEAMS = {
    "mens": {
        "label": "Men's",
        "sport_path": "mens-basketball",
        "db_name": "basketball.db",
        "secret": "password_men",
    },
    "womens": {
        "label": "Women's",
        "sport_path": "womens-basketball",
        "db_name": "basketball_women.db",
        "secret": "password_women",
    },
}
 
# Modules that keep their own DB_PATH. During a check they are all pointed at
# the chosen team's database, then put back.
PIPELINE_MODULES = (
    "roster",
    "box_score_stats",
    "parse_events",
    "compute_plus_minus",
    "stints",
    "season_stat",
)
 
_RUN_LOCK = threading.Lock()
 
 
# ----------------------------------------------------------------------------
# Small helpers
# ----------------------------------------------------------------------------
def _today() -> date:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/Chicago")).date()
    except Exception:
        return date.today()
 
 
def current_season_label(today: Optional[date] = None) -> str:
    """'2026-27' style label. Jul-Dec belong to the season starting that year."""
    today = today or _today()
    start = today.year if today.month >= 7 else today.year - 1
    return f"{start}-{str(start + 1)[-2:]}"
 
 
def season_range(first: str, today: Optional[date] = None) -> List[str]:
    """['2023-24', '2024-25', '2025-26'] from `first` through the current season."""
    m = re.fullmatch(r"(\d{4})-(\d{2})", first.strip())
    if not m:
        raise ValueError(f"'{first}' isn't a season like 2023-24")
    start = int(m.group(1))
    last = int(current_season_label(today).split("-")[0])
    return [f"{y}-{str(y + 1)[-2:]}" for y in range(start, last + 1)]
 
 
def season_label_for_date(game_date: str) -> str:
    """'YYYY-MM-DD' -> '2025-26'. Same rule as roster.get_season_for_game."""
    try:
        year, month, _ = (int(p) for p in str(game_date).split("-"))
    except (ValueError, TypeError):
        return "Unknown"
    start = year if month >= 7 else year - 1
    return f"{start}-{str(start + 1)[-2:]}"
 
 
def find_data_dir() -> str:
    """Same folder logic the dashboard uses to find basketball.db."""
    same = os.path.join(HERE, "basketball.db")
    up = os.path.join(HERE, "..", "data")
    if os.path.exists(same):
        return HERE
    return os.path.normpath(up)
 
 
def default_db_path(team: str) -> str:
    return os.path.join(find_data_dir(), TEAMS[team]["db_name"])
 
 
def get_team_password(secrets, team: str) -> Optional[str]:
    """The password for a team from Streamlit secrets, or None if not set.
    Men's also accepts the old single `password` secret, so the existing
    deployment keeps working until password_men is added."""
    keys = [TEAMS[team]["secret"]] + (["password"] if team == "mens" else [])
    for key in keys:
        try:
            value = secrets[key]
        except Exception:
            continue
        if isinstance(value, str) and value.strip():
            return value
    return None
 
 
# ----------------------------------------------------------------------------
# Reading the website
# ----------------------------------------------------------------------------
def _get(url: str, headers: Optional[Dict[str, str]] = None) -> requests.Response:
    last = None
    for attempt in range(1, REQUEST_TRIES + 1):
        try:
            resp = requests.get(url, headers=headers or REQUEST_HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code < 500:
                raise          # 404, 403 ... retrying won't help
            last = e
        except (requests.Timeout, requests.ConnectionError) as e:
            last = e
        _say(f"    no answer from the site (try {attempt} of {REQUEST_TRIES})")
    raise last
 
 
def site_season_labels(season: str) -> List[str]:
    """The site writes a season as '2025-26' in recent years but '2019-2020' in
    the older ones, so try both: ['2019-20', '2019-2020']."""
    m = re.fullmatch(r"(\d{4})-(\d{2})", season)
    return [season, f"{m.group(1)}-{int(m.group(1)) + 1}"] if m else [season]
 
 
def schedule_url(team: str, season: str) -> str:
    return f"{SITE}/sports/{TEAMS[team]['sport_path']}/schedule/{season}"
 
 
def find_box_score_links(html: str, sport_path: str, season: str) -> List[Dict]:
    """Box score page links on a schedule page, in page order, no duplicates."""
    pattern = re.compile(
        r"""href=["']([^"']*?/sports/%s/stats/%s/([^"'/]+)/boxscore/(\d+))["']"""
        % (re.escape(sport_path), re.escape(season))
    )
    seen, links = set(), []
    for m in pattern.finditer(html):
        box_id = m.group(3)
        if box_id in seen:
            continue
        seen.add(box_id)
        links.append({
            "id": box_id,
            "slug": m.group(2),
            "url": urljoin(SITE + "/", m.group(1)),
        })
    return links
 
 
def find_pdf_link(box_html: str, page_url: str) -> Optional[str]:
    """The 'View PDF' link on a box score page."""
    m = re.search(r"""href=["']([^"']*boxscore[^"']*?/pdf)["']""", box_html)
    return urljoin(page_url, m.group(1)) if m else None
 
 
def pdf_lines_from_bytes(content: bytes) -> List[str]:
    import pdfplumber  # imported here so the dashboard still starts without it
 
    lines: List[str] = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                for line in text.split("\n"):
                    line = line.strip()
                    if line:
                        lines.append(line)
    return lines
 
 
def _describe(resp: requests.Response) -> str:
    """One line saying what the server actually sent back (for error messages)."""
    ctype = resp.headers.get("content-type", "?").split(";")[0]
    try:
        html = resp.text
    except Exception:
        return f"HTTP {resp.status_code}, type {ctype}, starts: {resp.content[:80]!r}"
    title = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
    title = re.sub(r"\s+", " ", title.group(1)).strip() if title else "none"
    visible = re.sub(r"<(script|style)\b.*?</\1>", " ", html, flags=re.I | re.S)
    visible = re.sub(r"<[^>]+>", " ", visible)
    visible = re.sub(r"\s+", " ", visible).strip()
    hint = re.search(r"(not found|error|unavailable|sorry|denied|blocked|captcha|robot|verify|no pdf|"
                     r"does not exist|try again|expired)", visible, re.I)
    if hint:
        i = hint.start()
        said = visible[max(0, i - 80): i + 140]
    else:
        said = visible[:200]
    return f"HTTP {resp.status_code}, type {ctype}, page title {title!r}, page says: {said!r}"
 
 
_SKIP_EXT = (".js", ".css", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico", ".woff", ".woff2", ".webp")
 
 
def pdf_candidates(html: str, base_url: str) -> List[str]:
    """Every address in a page that could be the actual PDF file, best guesses
    first: the page's embedded viewer (<object>/<embed>/<iframe>), its
    'Download'/'Full Screen' links, then anything else that mentions pdf.
    File names may contain spaces and brackets, e.g. 'box score (1).pdf'."""
    text = (html.replace("\\/", "/").replace("\\u002F", "/").replace("\\u002f", "/")
                .replace("&amp;", "&"))
    primary: List[str] = []
    secondary: List[str] = []
 
    def clean(raw: str) -> Optional[str]:
        raw = raw.strip()
        if not raw or raw.startswith(("#", "javascript:", "data:", "mailto:")):
            return None
        url = urljoin(base_url, raw).split("#")[0]
        if url == base_url or url.split("?")[0].lower().endswith(_SKIP_EXT):
            return None
        return quote(url, safe=":/?&=%+@,;~()'!*[]$")      # spaces -> %20, no double-encoding
 
    def add(target: List[str], raw: str) -> None:
        url = clean(raw)
        if url and url not in primary and url not in secondary:
            target.append(url)
 
    # 1. the viewer embedded in the page
    for m in re.finditer(r"<(?:object|embed|iframe|source)\b[^>]*?\b(?:data|src)=\"([^\"]+)\"|"
                         r"<(?:object|embed|iframe|source)\b[^>]*?\b(?:data|src)='([^']+)'", text, re.I):
        raw = m.group(1) or m.group(2)
        if re.search(r"pdf", raw, re.I):
            add(primary, raw)
    # 2. 'View Full Screen' / 'Download' links, including download_file.ashx?file_location=<address>
    for m in re.finditer(r"<a\b[^>]*?href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", text, re.I | re.S):
        href, label = m.group(1), re.sub(r"<[^>]+>|\s+", " ", m.group(2))
        inner = re.search(r"[?&]file_location=([^&]+)", href)
        if inner:
            add(primary, unquote(inner.group(1)))
        if re.search(r"full ?screen|download|view pdf|open pdf", label, re.I) and re.search(r"pdf", href, re.I):
            add(primary, href)
    # 3. viewer.html?file=<address>
    for m in re.finditer(r"[?&](?:file|url|src|doc)=([^&\"'<>]+)", text, re.I):
        add(secondary, unquote(m.group(1)))
    # 4. anything quoted that mentions pdf
    for m in re.finditer(r"""["'(]([^"'()\s<>]*pdf[^"'()\s<>]*)["')]""", text, re.I):
        add(secondary, m.group(1))
    secondary.sort(key=lambda u: 0 if ".pdf" in u.lower().split("?")[0] else 1)
    return primary + secondary
 
 
def page_structure(html: str, base_url: str, limit: int = 30) -> List[str]:
    """Short list of the parts of a page that could hold or point to a file:
    frames/embeds, links named pdf/download/print, and file-hosting addresses.
    Used by --probe to show what a 'PDF page' is made of."""
    text = html.replace("\\/", "/").replace("\\u002F", "/").replace("&amp;", "&")
    out: List[str] = []
 
    def add(tag: str, value: str) -> None:
        line = f"{tag}: {urljoin(base_url, value.strip())}"
        if line not in out:
            out.append(line)
 
    for m in re.finditer(r"<(iframe|embed|object|source)\b[^>]*?\b(?:src|data)=[\"']([^\"']+)", text, re.I):
        add(m.group(1), m.group(2))
    for m in re.finditer(r"<a\b[^>]*?href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", text, re.I | re.S):
        label = re.sub(r"<[^>]+>|\s+", " ", m.group(2)).strip()
        if re.search(r"pdf|download|print|box ?score", label + m.group(1), re.I):
            add(f"link[{label[:30]}]", m.group(1))
    for m in re.finditer(r"""["'(]((?:https?:)?//[^"'()\s<>]*(?:cloudfront|sidearm|documents|download|files?)[^"'()\s<>]*)["')]""", text, re.I):
        add("address", m.group(1))
    for m in re.finditer(r"data-[a-z-]*(?:pdf|doc|file|url)[a-z-]*=[\"']([^\"']+)", text, re.I):
        add("data-attribute", m.group(1))
    return out[:limit]
 
 
def fetch_pdf_lines(pdf_url: str, referer: Optional[str] = None) -> List[str]:
    headers = dict(REQUEST_HEADERS)
    headers["Accept"] = "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8"
    if referer:
        headers["Referer"] = referer
 
    resp = _get(pdf_url, headers=headers)
    content = resp.content
    tried: List[str] = []
 
    if b"%PDF" not in content[:1024]:
        # The "PDF" link is often a web page that wraps the real file. Try each
        # address in the page that could be the file; the first real PDF wins.
        page_url = resp.url
        for cand in pdf_candidates(resp.text, page_url)[:12]:
            try:
                r2 = _get(cand, headers={**headers, "Referer": page_url})
            except Exception as e:
                tried.append(f"{cand} -> {e.__class__.__name__}")
                continue
            if b"%PDF" in r2.content[:1024]:
                resp, content = r2, r2.content
                break
            tried.append(f"{cand} -> {r2.headers.get('content-type', '?')}")
 
    if b"%PDF" not in content[:1024]:
        extra = (f"; also tried {len(tried)} other address(es) in the page, none was a PDF"
                 if tried else "; the page has no other address that looks like a PDF")
        raise ValueError(f"{pdf_url} did not return a PDF ({_describe(resp)}{extra})")
 
    content = content[content.index(b"%PDF"):]
    global last_pdf_source
    last_pdf_source = resp.url
    return pdf_lines_from_bytes(content)
 
 
# ----------------------------------------------------------------------------
# Reading the PDF header
# ----------------------------------------------------------------------------
_HEADER_RE = re.compile(
    r"^(?P<a>.+?)\s*\([^()]*\d+-\d+[^()]*\)\s*-vs-\s*(?P<b>.+?)\s*\([^()]*\d+-\d+[^()]*\)\s*$"
)
_DATE_RE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{2,4})\b")
 
 
# The same school is printed several ways depending on the season and the PDF:
# "UW-River Falls", "Wis.-River Falls", "WIS-River Falls", "Wisconsin-River Falls",
# "Univ. of Wisconsin-River Falls". Everything else (roster, starters, scores,
# stints, the dashboard) looks for "UW-River Falls", so we write it that way
# when a game is imported.
_SCHOOL_RE = re.compile(
    r"(?:Univ(?:ersity|\.)?\s+of\s+)?(?:UWRF|(?:UW|Wis(?:consin)?\.?)\s*[-\u2013.]?\s*River\s+Falls)",
    re.I,
)
 
 
_ROW_RE = re.compile(
    r"^(?P<num>\d+)\s+(?P<name>[A-Za-z][^\d*,]*?)(?P<star>\s+\*)?\s+"
    r"(?P<rest>\d+\+?\s+\d+-\d+\s+\d+-\d+\s+\d+-\d+\s+\d+-\d+\s.*)$")
 
 
_PARTICLES = ("van", "von", "der", "ten", "ter", "de", "del", "di", "la", "le")
_GLUED_RE = re.compile(r"^(\d+\s+)(?=[A-Za-z\-']+,[A-Z][a-z]+(?:Van|Von|Der|Ten|Ter)\b)([A-Za-z\-']+),([A-Z][a-z]+)(Van|Von|Der|Ten|Ter)(?=\s)")
 
 
def normalize_player_names(lines: List[str]) -> List[str]:
    """Some box scores print names as 'Logan Jedwabny' (first name first, no comma)
    instead of 'Jedwabny,Logan', and a few put a space before the comma
    ('Reeck ,Jake'). The play-by-play always uses 'LAST,FIRST', so rewrite the box
    score rows to that one style. 'Will Van Scoy' becomes 'Scoy,WillVan' (the
    play-by-play writes SCOY,WILL VAN and is matched to it by parse_events)."""
    out = []
    for line in lines:
        line = re.sub(r"(?<=[A-Za-z]) +,(?=[A-Za-z])", ",", line)
        m = _ROW_RE.match(line.strip())
        if m and m.group("name").strip().upper() not in ("TEAM", "TOTALS"):
            words = m.group("name").split()
            if len(words) >= 2:
                suffix = ""
                if words[-1].rstrip(".").upper() in ("JR", "SR", "II", "III", "IV") and len(words) >= 3:
                    suffix = words.pop()
                last = words.pop()
                if words and words[-1].lower() in _PARTICLES:      # 'Will Van Scoy' -> 'Van Scoy,Will'
                    last = f"{words.pop()} {last}"
                name = f"{last},{''.join(words)}{suffix}"
                line = f"{m.group('num')} {name}{m.group('star') or ''} {m.group('rest')}"
        else:
            # earlier versions wrote 'Scoy,WillVan'; put the particle back on the last name
            line = _GLUED_RE.sub(lambda g: f"{g.group(1)}{g.group(4)} {g.group(2)},{g.group(3)}", line)
        out.append(line)
    return out
 
 
def normalize_school_name(lines: List[str]) -> List[str]:
    return [_SCHOOL_RE.sub("UW-River Falls", line) for line in lines]
 
 
def _clean_name(name: str) -> str:
    """'Wis.-La Crosse' -> 'LaCrosse', 'Central (IA)' -> 'CentralIA' - the same
    style the existing games use (the dashboard splits the capital letters back
    into words)."""
    name = re.sub(r"^Wis\.?[-\s]*", "", name.strip())
    return re.sub(r"[^A-Za-z0-9]", "", name)
 
 
def parse_game_header(lines: List[str]) -> Dict[str, str]:
    """game_date, opponent, location from the first lines of a box score PDF.
    Raises ValueError if the header is not in the expected shape."""
    head = lines[:8]
 
    teams = None
    for line in head:
        m = _HEADER_RE.match(line)
        if m:
            teams = (m.group("a").strip(), m.group("b").strip())
            break
    if not teams:
        raise ValueError("could not find the 'Team -vs- Team' line")
 
    game_date = None
    for line in head:
        m = _DATE_RE.match(line)
        if m:
            month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
            year += 2000 if year < 100 else 0
            game_date = f"{year:04d}-{month:02d}-{day:02d}"
            break
    if not game_date:
        raise ValueError("could not find the game date")
 
    first, second = teams
    if roster.is_uwrf_team_label(second):
        location, opponent = "Home", first
    elif roster.is_uwrf_team_label(first):
        location, opponent = "Away", second
    else:
        raise ValueError("neither team is UW-River Falls")
 
    return {"game_date": game_date, "opponent": _clean_name(opponent), "location": location}
 
 
# ----------------------------------------------------------------------------
# Database setup
# ----------------------------------------------------------------------------
def create_schema(db_path: str) -> None:
    """Build all tables in a new database by running the project's own
    create_database.py, pointed at db_path (so there is only one schema)."""
    src_path = os.path.join(HERE, "create_database.py")
    with open(src_path, encoding="utf-8") as f:
        src = f.read()
    literal = 'sqlite3.connect("data/basketball.db")'
    if literal not in src:
        raise RuntimeError("create_database.py no longer has its usual connect line")
    src = src.replace(literal, f"sqlite3.connect({db_path!r})")
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    with contextlib.redirect_stdout(io.StringIO()):
        exec(compile(src, src_path, "exec"), {"__name__": "create_database_exec"})
 
 
def ensure_db(db_path: str) -> None:
    needs_schema = not os.path.exists(db_path)
    if not needs_schema:
        c = sqlite3.connect(db_path)
        needs_schema = c.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='games'"
        ).fetchone() is None
        c.close()
    if needs_schema:
        create_schema(db_path)
 
    conn = sqlite3.connect(db_path)
    roster.ensure_roster_table(conn)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scraped_box_scores (
            box_id  TEXT PRIMARY KEY,
            season  TEXT,
            pdf_url TEXT,
            game_id INTEGER,
            status  TEXT
        )
    """)
    conn.commit()
    conn.close()
 
 
# ----------------------------------------------------------------------------
# Rebuilding stats after new games are added
# ----------------------------------------------------------------------------
@contextlib.contextmanager
def _pointed_at(db_path: str):
    """Make every pipeline module use `db_path` for the duration, then restore."""
    mods = {name: importlib.import_module(name) for name in PIPELINE_MODULES}
    saved = {}
    for name, mod in mods.items():
        if hasattr(mod, "DB_PATH"):
            saved[name] = mod.DB_PATH
            mod.DB_PATH = db_path
    try:
        yield mods
    finally:
        for name, value in saved.items():
            mods[name].DB_PATH = value
 
 
def run_pipeline(db_path: str, new_game_ids: List[int]) -> str:
    """The same steps run_all.py runs after importing (roster, events,
    plus/minus, box score stats, stints, season stats). Returns the text the
    steps printed."""
    buf = io.StringIO()
    with _pointed_at(db_path) as mods, contextlib.redirect_stdout(buf):
        mods["box_score_stats"].build_roster_for_all_games()
        for game_id in new_game_ids:
            mods["parse_events"].parse_game_events(game_id=game_id, delete_existing=True)
        mods["compute_plus_minus"].compute_all_games_plus_minus()
        mods["box_score_stats"].parse_all_games()
        mods["stints"].compute_all_games_stints()
        mods["season_stat"].compute_season_stats()
    return buf.getvalue()
 
 
def drop_unprocessable_games(db_path: str, new_game_ids: List[int], log: str) -> Dict[int, Dict[str, str]]:
    """A game whose stats could not be built (e.g. a PDF laid out differently
    from the usual one) would sit on the dashboard with no player numbers.
    Remove those games again so the dashboard only shows complete games, and
    return {game_id: {"reason", "file"}} so the problem can be reported. The
    game's PDF text is saved in a 'failed_games' folder next to the database
    so the cause can be looked at."""
    conn = sqlite3.connect(db_path)
    bad: Dict[int, Dict[str, str]] = {}
    try:
        for gid in new_game_ids:
            built = conn.execute(
                "SELECT COUNT(*) FROM player_game_stats WHERE game_id=? AND min_played IS NOT NULL",
                (gid,),
            ).fetchone()[0]
            if built:
                continue        # (a game with no play-by-play keeps its box score stats only)
            m = re.search(rf"ERROR in game {gid}: (.+)", log)
            reason = m.group(1).strip() if m else "no player stats could be read from its box score"
            saved = ""
            try:
                row = conn.execute("SELECT game_date, opponent FROM games WHERE game_id=?", (gid,)).fetchone()
                lines = [r[0] for r in conn.execute(
                    "SELECT line_text FROM raw_lines WHERE game_id=? ORDER BY line_id", (gid,))]
                folder = os.path.join(os.path.dirname(os.path.abspath(db_path)), "failed_games")
                os.makedirs(folder, exist_ok=True)
                saved = os.path.join(folder, f"{row[0]}_{row[1]}.txt")
                with open(saved, "w", encoding="utf-8") as f:
                    f.write(f"# {reason}\n")
                    f.write("\n".join(lines))
            except Exception:
                saved = ""
            bad[gid] = {"reason": reason, "file": saved}
        for gid in bad:
            for table in ("lineup_states", "events", "player_game_stats", "raw_lines",
                          "roster_appearances", "scraped_box_scores", "games"):
                conn.execute(f"DELETE FROM {table} WHERE game_id=?", (gid,))
        conn.commit()
    finally:
        conn.close()
    if bad:
        with _pointed_at(db_path) as mods, contextlib.redirect_stdout(io.StringIO()):
            mods["season_stat"].compute_season_stats()
    return bad
 
 
# ----------------------------------------------------------------------------
# The check itself
# ----------------------------------------------------------------------------
def check_for_new_games(
    team: str,
    db_path: str,
    season: Optional[str] = None,
    today: Optional[date] = None,
    include_today: bool = False,
) -> Dict:
    """Find, import and process any new finished games. Returns a dict with
    what happened (imported / pending / errors / ...). Never raises for a
    problem with one game - that game is reported and retried next time."""
    cfg = TEAMS[team]
    today = today or _today()
    season = season or current_season_label(today)
    result = {
        "season": season,
        "found": 0,
        "already_have": 0,
        "skipped_exhibition": 0,
        "imported": [],
        "pending": [],
        "errors": [],
        "pipeline_log": "",
    }
 
    ensure_db(db_path)
 
    _say(f"Opening the {season} schedule page...")
    links, first_error = [], None
    for label in site_season_labels(season):
        try:
            schedule_html = _get(schedule_url(team, label)).text
        except Exception as e:
            first_error = first_error or e
            continue
        links = find_box_score_links(schedule_html, cfg["sport_path"], label)
        if links:
            break
    if not links and first_error is not None:
        result["errors"].append(f"Couldn't open the {season} schedule page: {first_error}")
        return result
    result["found"] = len(links)
    _say(f"Found {len(links)} box score links on the schedule page.")
    if not links:
        return result
 
    # imported lazily: import_game needs pdfplumber, which is only needed here
    from import_game import detect_conference_game, insert_game, insert_raw_lines
 
    conn = sqlite3.connect(db_path)
    try:
        recorded = {r[0] for r in conn.execute("SELECT box_id FROM scraped_box_scores")}
        known_dates = {r[0]: r[1] for r in conn.execute("SELECT game_date, game_id FROM games")}
        new_ids: List[int] = []
 
        for n, link in enumerate(links, start=1):
            box_id = link["id"]
            if box_id not in recorded:
                _say(f"[{n}/{len(links)}] looking at {link['slug']} ...")
            if box_id in recorded:
                result["already_have"] += 1
                continue
 
            if "exhibition" in link["slug"].lower():
                conn.execute(
                    "INSERT OR REPLACE INTO scraped_box_scores VALUES (?,?,?,?,?)",
                    (box_id, season, None, None, "exhibition"),
                )
                conn.commit()
                result["skipped_exhibition"] += 1
                continue
 
            try:
                pdf_url = find_pdf_link(_get(link["url"]).text, link["url"])
                if not pdf_url:
                    result["pending"].append(f"{link['slug']}: no PDF posted yet")
                    continue
 
                lines = normalize_player_names(normalize_school_name(fetch_pdf_lines(pdf_url, referer=link["url"])))
                header = parse_game_header(lines)
 
                game_day = datetime.strptime(header["game_date"], "%Y-%m-%d").date()
                if game_day >= today and not include_today:
                    result["pending"].append(
                        f"{header['game_date']} {header['opponent']}: played today, "
                        "will be added on the next check after today"
                    )
                    continue
 
                if header["game_date"] in known_dates:
                    conn.execute(
                        "INSERT OR REPLACE INTO scraped_box_scores VALUES (?,?,?,?,?)",
                        (box_id, season, pdf_url, known_dates[header["game_date"]], "existing"),
                    )
                    conn.commit()
                    result["already_have"] += 1
                    continue
 
                with contextlib.redirect_stdout(io.StringIO()):
                    conference = detect_conference_game(lines)
                game_id = insert_game(
                    conn,
                    game_date=header["game_date"],
                    opponent=header["opponent"],
                    location=header["location"],
                    source_url=pdf_url,
                    conference=conference,
                )
                insert_raw_lines(conn, game_id, lines)
                conn.execute(
                    "INSERT OR REPLACE INTO scraped_box_scores VALUES (?,?,?,?,?)",
                    (box_id, season, pdf_url, game_id, "imported"),
                )
                conn.commit()
                known_dates[header["game_date"]] = game_id
                new_ids.append(game_id)
                result["imported"].append({
                    "game_id": game_id,
                    "date": header["game_date"],
                    "opponent": header["opponent"],
                    "location": header["location"],
                    "box_score_only": not any("play by play" in l.lower() for l in lines),
                })
            except Exception as e:
                result["errors"].append(f"{link['slug']} (box score {box_id}): {e}")
    finally:
        conn.close()
 
    if new_ids:
        result["pipeline_log"] = run_pipeline(db_path, new_ids)
        bad = drop_unprocessable_games(db_path, new_ids, result["pipeline_log"])
        if bad:
            kept = []
            for g in result["imported"]:
                if g["game_id"] in bad:
                    info = bad[g["game_id"]]
                    where = f" Its text was saved to {info['file']}." if info["file"] else ""
                    result["errors"].append(
                        f"{g['date']} {g['opponent']}: downloaded, but its stats couldn't be built "
                        f"({info['reason']}), so it was not added.{where}"
                    )
                else:
                    kept.append(g)
            result["imported"] = kept
    return result
 
 
def summarize(result: Dict) -> List[str]:
    """Plain-language lines for the dashboard / terminal."""
    lines = []
    n = len(result["imported"])
    if result["found"] == 0 and not result["errors"]:
        lines.append(f"No box scores are posted yet for {result['season']}.")
    elif n:
        lines.append(f"Added {n} new game{'s' if n != 1 else ''}:")
        for g in result["imported"]:
            where = "vs" if g["location"] == "Home" else "at"
            note = "  (box score only - no play-by-play posted, so no plus/minus)" if g.get("box_score_only") else ""
            lines.append(f"  {g['date']}  {where} {g['opponent']}{note}")
    elif not result["errors"] and not result["pending"]:
        lines.append("No new games - everything posted so far is already in.")
    if result["skipped_exhibition"]:
        lines.append(f"Skipped {result['skipped_exhibition']} exhibition game(s).")
    for p in result["pending"]:
        lines.append(f"Waiting: {p}")
    for e in result["errors"]:
        lines.append(f"Problem: {e}")
    return lines
 
 
# ----------------------------------------------------------------------------
# Check + save to GitHub (what the dashboard button calls)
# ----------------------------------------------------------------------------
def run_check(team: str, secrets=None, season: Optional[str] = None,
              db_path: Optional[str] = None, today: Optional[date] = None) -> Dict:
    """Pull the latest database from GitHub, check for new games, and push the
    updated database back if anything was added. `secrets` is st.secrets (or
    any dict); without a github_token the check still runs but nothing is
    saved. Only one check can run at a time."""
    import github_sync
 
    out = {"ok": True, "messages": [], "imported": [], "saved": None, "log": "", "result": None}
 
    if not _RUN_LOCK.acquire(blocking=False):
        out["ok"] = False
        out["messages"].append("Another check is already running - try again in a minute.")
        return out
 
    try:
        db_path = db_path or default_db_path(team)
        gh = github_sync.config_from_secrets(secrets) if secrets is not None else None
        repo_path, sha = None, None
 
        if gh:
            repo_path = github_sync.repo_path_for(gh, TEAMS[team]["db_name"])
            try:
                sha = github_sync.pull_db(gh, repo_path, db_path)
                if sha is None:
                    # No copy on GitHub yet. That is only expected for the women's
                    # database's very first save, and only if the men's database is
                    # where we think it is. Otherwise github_data_dir is wrong, and
                    # saving would create a stray copy the dashboard never reads.
                    mens_path = github_sync.repo_path_for(gh, TEAMS["mens"]["db_name"])
                    if team == "mens" or not github_sync.file_exists(gh, mens_path):
                        raise github_sync.SyncError(
                            f"{mens_path} isn't in {gh['repo']} ({gh['branch']}). Check the "
                            "github_data_dir secret - it should be the folder that holds "
                            "basketball.db in the repo (\".\" for the top level)."
                        )
            except github_sync.SyncError as e:
                out["ok"] = False
                out["messages"].append(f"Couldn't get the latest data from GitHub: {e}")
                return out
 
        result = check_for_new_games(team, db_path, season=season, today=today)
        out["result"] = result
        out["imported"] = result["imported"]
        out["log"] = result["pipeline_log"]
        out["messages"] = summarize(result)
        if result["errors"]:
            out["ok"] = False
 
        if result["imported"]:
            n = len(result["imported"])
            if gh:
                try:
                    github_sync.push_db(
                        gh, repo_path, db_path, sha,
                        message=f"Add {n} new {TEAMS[team]['label'].lower()} game(s) from uwrfsports.com (dashboard check)",
                    )
                    out["saved"] = True
                    out["messages"].append(
                        "Saved to GitHub. The app restarts to pick it up, so the page may reload "
                        "in a minute or two - you'll need to log in again."
                    )
                except github_sync.SyncError as e:
                    out["saved"] = False
                    out["ok"] = False
                    out["messages"].append(f"The games were added here but could NOT be saved to GitHub: {e}")
            else:
                out["saved"] = False
                out["messages"].append(
                    "Not saved: no github_token is set, so these games will disappear when the app restarts."
                )
        return out
    finally:
        _RUN_LOCK.release()
 
 
if __name__ == "__main__":
    import argparse
 
    parser = argparse.ArgumentParser(description="Check uwrfsports.com for new games.")
    parser.add_argument("team", choices=sorted(TEAMS))
    parser.add_argument("--season", nargs="+", metavar="SEASON",
                        help="one or more seasons, e.g. 2025-26 or 2023-24 2024-25 (default: the current season)")
    parser.add_argument("--from", dest="from_season", metavar="SEASON",
                        help="every season from this one up to the current one, e.g. --from 2022-23")
    parser.add_argument("--db", help="database file (default: the project's data folder)")
    parser.add_argument("--verbose", action="store_true", help="also print the stat-rebuild output")
    parser.add_argument("--probe", metavar="PDF_URL",
                        help="just test one PDF link and show what comes back (changes nothing)")
    args = parser.parse_args()
    progress_printer = lambda text: print(text, flush=True)
    globals()["progress"] = progress_printer
 
    if args.probe:
        try:
            _lines = normalize_player_names(normalize_school_name(fetch_pdf_lines(args.probe)))
            print(f"It is a PDF with {len(_lines)} lines of text.")
            print(f"The file really came from: {last_pdf_source}")
            print("First lines:")
            for _l in _lines[:6]:
                print("   ", _l)
            print("Read as:", parse_game_header(_lines))
        except Exception as e:
            print("Problem:", e)
            try:
                _r = _get(args.probe)
                print("\nWhat the page contains that could point to a file:")
                for _line in page_structure(_r.text, _r.url) or ["(nothing found)"]:
                    print("   ", _line)
            except Exception as e2:
                print("(couldn't re-open the page to inspect it:", e2, ")")
        raise SystemExit
 
    db = args.db or default_db_path(args.team)
    print(f"Database: {db}")
 
    if args.from_season:
        seasons = season_range(args.from_season)
    else:
        seasons = list(args.season) if args.season else [None]
    seasons = sorted(set(seasons), key=lambda x: x or "9999")     # oldest first
 
    total_added, total_problems = 0, 0
    for season in seasons:
        res = check_for_new_games(args.team, db, season=season)
        print(f"\n=== {res['season']} ===")
        for line in summarize(res):
            print(line)
        if args.verbose and res["pipeline_log"]:
            print(res["pipeline_log"])
        total_added += len(res["imported"])
        total_problems += len(res["errors"])
    if len(seasons) > 1:
        print(f"\nAll done: {total_added} games added, {total_problems} problem(s).")
 