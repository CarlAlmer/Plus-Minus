import sqlite3
import requests
import pdfplumber
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
                lines = text.split("\n")

                for line in lines:
                    cleaned_line = line.strip()

                    if cleaned_line:
                        all_lines.append(cleaned_line)

    return all_lines


def connect_db():
    return sqlite3.connect("data/basketball.db")


def insert_game(conn, game_date=None, opponent=None, location=None, source_url=None):
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO games (game_date, opponent, location, source_url)
        VALUES (?, ?, ?, ?)
    """, (game_date, opponent, location, source_url))

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

    conn = connect_db()

    print("Creating game record...")
    game_id = insert_game(
        conn=conn,
        game_date=game_date,
        opponent=opponent,
        location=location,
        source_url=pdf_url
    )

    print(f"Created game_id = {game_id}")

    print("Saving raw lines to database...")
    insert_raw_lines(conn, game_id, lines)

    conn.close()

    print("Import complete.")
