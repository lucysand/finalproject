import sqlite3

DB_PATH = "nyc_data.db"
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# List all tables
cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cur.fetchall()
print(tables)

conn.close()
