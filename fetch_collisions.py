import requests
import sqlite3
import time

DB_PATH = "nyc_data.db"   # change if needed
TABLE_NAME = "collisions"

API_URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"
BATCH_SIZE = 25
MAX_BATCHES = 5   # 20 * 500 = 10,000 rows

def create_table():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            collision_id INTEGER PRIMARY KEY,
            borough TEXT,
            persons_injured INTEGER
        )
    """)
    conn.commit()
    conn.close()

def fetch_collisions(offset):
    params = {
        "$limit": BATCH_SIZE,
        "$offset": offset,
        "$select": "collision_id, borough, number_of_persons_injured",
        "$where": "borough IS NOT NULL"
    }
    resp = requests.get(API_URL, params=params)
    resp.raise_for_status()
    return resp.json()

def insert_rows(rows):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    inserted = 0

    for r in rows:
        cid = r.get("collision_id")
        borough = r.get("borough")
        injured = r.get("number_of_persons_injured") or 0

        if not cid or not borough:
            continue

        cur.execute(f"""
            INSERT INTO {TABLE_NAME}(collision_id, borough, persons_injured)
            VALUES (?, ?, ?)
            ON CONFLICT(collision_id)
            DO UPDATE SET 
                borough=excluded.borough,
                persons_injured=excluded.persons_injured
        """, (cid, borough, injured))

        inserted += 1

    conn.commit()
    conn.close()
    return inserted

if __name__ == "__main__":
    create_table()

    offset = 0
    for i in range(MAX_BATCHES):
        print(f"Fetching batch {i+1}/{MAX_BATCHES} (offset {offset})...")
        rows = fetch_collisions(offset)
        if not rows:
            print("No more rows.")
            break

        count = insert_rows(rows)
        print(f"Inserted/updated {count} rows.")

        offset += BATCH_SIZE
        time.sleep(0.2)

    print("Collisions import complete.")
