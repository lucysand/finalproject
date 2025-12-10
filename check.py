import sqlite3

conn = sqlite3.connect("/Users/lucysanders/Desktop/SI201/finalproject/nyc_data.db")
cur = conn.cursor()

cur.execute("PRAGMA table_info(demographics);")
for col in cur.fetchall():
    print(col)

conn.close()