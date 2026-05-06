import sqlite3
import requests
import pdfplumber
import re
from io import BytesIO


def extract_lines_from_pdf_url(pdf_url):
    response = requests.get(pdf_url)
    response.raise_for_status()

    pdf_file = BytesIO(response.content)
    all_lines = []

    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()

            if text:
                for line in text.split("\n"):
                    cleaned_line = line.strip()
                    if cleaned_line:
                        all_lines.append(cleaned_line)

    return all_lines


def connect_db():
    return sqlite3.connect("basketball.db")


def normalize_conference(conf):
    return re.sub(r"\s+", " ", conf.strip()).upper()


def detect_conference_game(lines):
    """
    Handles:
      (1-0,0-0 American Rivers)
      (0-1,0-0 WIAC)
      (2-6,0-0 UMAC)

    Returns:
      1 if conferences match
      0 if conferences do not match
      None if it cannot detect both
    """

    top_text = " ".join(lines[:5])

    conference_matches = re.findall(
        r"\(\s*\d+-\d+\s*,\s*\d+-\d+\s+([A-Za-z][A-Za-z\s.&'-]*?)\s*\)",
        top_text
    )

    if len(conference_matches) < 2:
        print("WARNING: Could not detect both team conferences.")
        return None

    team1_conference = normalize_conference(conference_matches[0])
    team2_conference = normalize_conference(conference_matches[1])

    is_conference_game = team1_conference == team2_conference

    print(
        f"Detected conferences: {team1_conference} vs {team2_conference} "
        f"-> conference game = {is_conference_game}"
    )

    return 1 if is_conference_game else 0


def insert_game(conn, game_date=None, opponent=None, location=None, source_url=None, conference=None):
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO games (game_date, opponent, location, source_url, conference)
        VALUES (?, ?, ?, ?, ?)
    """, (game_date, opponent, location, source_url, conference))

    conn.commit()
    return cursor.lastrowid


def insert_raw_lines(conn, game_id, lines):
    cursor = conn.cursor()

    for line in lines:
        cursor.execute("""
            INSERT INTO raw_lines (game_id, line_text)
            VALUES (?, ?)
        """, (game_id, line))

    conn.commit()


def import_game_from_pdf_url(pdf_url, game_date=None, opponent=None, location=None):
    print("Reading PDF from URL...")
    lines = extract_lines_from_pdf_url(pdf_url)
    print(f"Extracted {len(lines)} lines.")

    conference = detect_conference_game(lines)

    conn = connect_db()

    print("Creating game record...")
    game_id = insert_game(
        conn=conn,
        game_date=game_date,
        opponent=opponent,
        location=location,
        source_url=pdf_url,
        conference=conference
    )

    print(f"Created game_id = {game_id}")

    print("Saving raw lines to database...")
    insert_raw_lines(conn, game_id, lines)

    conn.close()

    print("Import complete.")
