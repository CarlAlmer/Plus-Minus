import requests
import time
from datetime import datetime

from import_game import import_game_from_pdf_url, connect_db


# ---------------------------------------
# CONFIG
# ---------------------------------------
REPO_OWNER = "CarlAlmer"
REPO_NAME = "Plus-Minus"
FOLDER_PATH = "play_by_play_pdf"


# ---------------------------------------
# GET PDF URLS FROM GITHUB
# ---------------------------------------
def get_pdf_urls_from_github():
    api_url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{FOLDER_PATH}"

    response = requests.get(api_url)
    response.raise_for_status()

    files = response.json()

    pdf_files = []

    for file in files:
        if file["name"].lower().endswith(".pdf"):
            pdf_files.append({
                "name": file["name"],
                "url": file["download_url"]
            })

    return pdf_files


# ---------------------------------------
# CHECK IF GAME ALREADY EXISTS
# ---------------------------------------
def game_exists(conn, source_url):
    cursor = conn.cursor()
    cursor.execute(
        "SELECT game_id FROM games WHERE source_url = ?",
        (source_url,)
    )
    return cursor.fetchone() is not None


# ---------------------------------------
# EXTRACT METADATA FROM FILENAME
# ---------------------------------------
def extract_metadata(filename):
    """
    Extracts:
    - game_date (YYYY-MM-DD)
    - opponent
    - location (Home/Away)

    Expected formats:
    - 20251108_vs_CentralIA.pdf
    - 20251109_at_Superior.pdf
    """

    import os
    from datetime import datetime

    # Remove .pdf
    name = os.path.splitext(filename)[0]

    parts = name.split("_")

    game_date = None
    opponent = "Unknown"
    location = "Unknown"

    try:
        # ---- DATE ----
        date_str = parts[0]
        game_date = datetime.strptime(date_str, "%Y%m%d").date()

        # ---- LOCATION ----
        if parts[1].lower() == "vs":
            location = "Home"
        elif parts[1].lower() == "at":
            location = "Away"

        # ---- OPPONENT ----
        opponent = parts[2]

    except Exception as e:
        print(f"Metadata parsing failed for {filename}: {e}")

    return (
        str(game_date) if game_date else None,
        opponent,
        location
    )


# ---------------------------------------
# MAIN BULK IMPORT
# ---------------------------------------
def run_bulk_import():
    print("Fetching PDFs from GitHub...")
    pdf_files = get_pdf_urls_from_github()

    print(f"Found {len(pdf_files)} PDF files.\n")

    conn = connect_db()

    for i, file in enumerate(pdf_files, start=1):
        filename = file["name"]
        url = file["url"]

        print(f"[{i}/{len(pdf_files)}] Processing: {filename}")

        # Skip duplicates
        if game_exists(conn, url):
            print("   -> Already imported. Skipping.\n")
            continue

        game_date, opponent, location = extract_metadata(filename)

        try:
            import_game_from_pdf_url(
                pdf_url=url,
                game_date=game_date,
                opponent=opponent,
                location=location
            )
        except Exception as e:
            print(f"   -> ERROR: {e}")

        # Be nice to GitHub API
        time.sleep(0.5)

    conn.close()
    print("\nBulk import complete.")


# ---------------------------------------
# RUN
# ---------------------------------------
if __name__ == "__main__":
    run_bulk_import()