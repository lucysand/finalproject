import requests
import sqlite3
import time

DB_PATH = "nyc_data.db"
COLLISIONS_API = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"

BATCH_SIZE = 25
MAX_BATCHES = 5

def get_connection():
    return sqlite3.connect(DB_PATH, timeout=10)

def create_tables():
    conn = get_connection()
    cur = conn.cursor()

    # Collisions table (general info)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS collisions (
            collision_id INTEGER PRIMARY KEY,
            borough TEXT
        )
    """)

    # Collision injuries table (linked via integer key)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS collision_injuries (
            collision_id INTEGER PRIMARY KEY,
            persons_injured INTEGER,
            FOREIGN KEY(collision_id) REFERENCES collisions(collision_id)
        )
    """)

    conn.commit()
    conn.close()

def insert_collision_data(batch_data):
    conn = get_connection()
    cur = conn.cursor()

    for row in batch_data:
        collision_id = int(row.get("collision_id"))
        borough = row.get("borough")
        persons_injured = int(row.get("number_of_persons_injured", 0))

        # Insert into collisions
        cur.execute("""
            INSERT OR IGNORE INTO collisions (collision_id, borough)
            VALUES (?, ?)
        """, (collision_id, borough))

        # Insert into collision_injuries
        cur.execute("""
            INSERT OR IGNORE INTO collision_injuries (collision_id, persons_injured)
            VALUES (?, ?)
        """, (collision_id, persons_injured))

    conn.commit()
    conn.close()

def fetch_collisions(offset=0, limit=25):
    params = {
        "$limit": limit,
        "$offset": offset
    }
    response = requests.get(COLLISIONS_API, params=params)
    response.raise_for_status()
    return response.json()

def main():
    create_tables()
    for batch_num in range(MAX_BATCHES):
        offset = batch_num * BATCH_SIZE
        print(f"Fetching batch {batch_num+1} (offset {offset})...")
        data = fetch_collisions(offset=offset, limit=BATCH_SIZE)
        insert_collision_data(data)
        time.sleep(1)  # be kind to the API

    print("Finished fetching collision data.")

if __name__ == "__main__":
    main()
