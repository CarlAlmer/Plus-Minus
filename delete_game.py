import sqlite3

conn = sqlite3.connect("data/basketball.db")
cursor = conn.cursor()

wrong_game_id = 2

cursor.execute("DELETE FROM raw_lines WHERE game_id = ?", (wrong_game_id,))
cursor.execute("DELETE FROM games WHERE game_id = ?", (wrong_game_id,))

conn.commit()
conn.close()

print("Wrong game and its raw lines deleted.")