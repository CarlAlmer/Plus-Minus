import sqlite3

conn = sqlite3.connect("data/basketball.db")
cursor = conn.cursor()

cursor.execute("DROP TABLE lineup_states;")

conn.commit()
conn.close()

print("Done")